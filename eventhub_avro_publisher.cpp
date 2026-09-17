#include <azure/identity.hpp>
#include <azure/messaging/eventhubs.hpp>

// Avro C++ 1.12.1's Exception.hh calls fmt::format without including the
// complete fmt formatting API. Include it first to keep Avro headers usable.
#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

// End-to-end flow:
//   1. Generate a StockTick C++ object.
//   2. Serialize it as a one-record Avro Object Container File (OCF).
//   3. Put those OCF bytes in one EventData message.
//   4. Group independent EventData messages into an Event Hubs transport batch.
//   5. Send with Microsoft Entra authentication through DefaultAzureCredential.
//
// The distinction between an Avro container and an EventDataBatch matters:
// every EventData body remains a self-contained Avro document that Eventhouse
// can decode independently; the EventDataBatch only reduces network calls.

namespace sample {

// The member order must match the field order used by codec_traits below.
// eventtime stores Unix epoch milliseconds to match Avro timestamp-millis.
struct StockTick {
    std::string eventname;
    std::int64_t eventtime;
    std::string ticker;
    double price;
    std::string eventdesc;
};

}  // namespace sample

namespace avro {

// Avro uses codec_traits to map the C++ record to fields in the writer schema.
// decode is used only by the optional local round-trip validation in this app.
template <>
struct codec_traits<sample::StockTick> {
    static void encode(Encoder& encoder, const sample::StockTick& value) {
        avro::encode(encoder, value.eventname);
        avro::encode(encoder, value.eventtime);
        avro::encode(encoder, value.ticker);
        avro::encode(encoder, value.price);
        avro::encode(encoder, value.eventdesc);
    }

    static void decode(Decoder& decoder, sample::StockTick& value) {
        avro::decode(decoder, value.eventname);
        avro::decode(decoder, value.eventtime);
        avro::decode(decoder, value.ticker);
        avro::decode(decoder, value.price);
        avro::decode(decoder, value.eventdesc);
    }
};

}  // namespace avro

namespace {

constexpr std::string_view kSchemaName = "sample.StockTick";

// The writer schema is embedded in every OCF payload. That makes each EventData
// self-describing, which is required by the validated direct Eventhouse Avro
// ingestion path used by this sample.
constexpr const char* kAvroSchema = R"({
  "type": "record",
  "name": "StockTick",
  "namespace": "sample",
  "fields": [
    {"name": "eventname", "type": "string"},
    {"name": "eventtime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ticker", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "eventdesc", "type": "string"}
  ]
})";

struct TickerState {
    std::string symbol;
    double base_price;
    double price;
};

struct Options {
    // count is the total number of EventData messages, while batch_size limits
    // how many independent messages are grouped into each service send.
    int count = 10;
    int batch_size = 1;
    int interval_ms = 100;
    bool validate_payload = false;
};

struct PublishedEvent {
    sample::StockTick tick;
    std::size_t payload_size;
};

std::string require_environment_variable(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error(std::string(name) + " is required");
    }
    return value;
}

int parse_positive_int(const char* value, std::string_view option) {
    const std::string_view input(value);
    int parsed = 0;
    const auto [end, error] =
        std::from_chars(input.data(), input.data() + input.size(), parsed);
    if (error != std::errc{} || end != input.data() + input.size() ||
        parsed <= 0) {
        throw std::invalid_argument(
            std::string(option) + " must be a positive integer");
    }
    return parsed;
}

Options parse_options(int argc, char* argv[]) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--count" && index + 1 < argc) {
            options.count = parse_positive_int(argv[++index], "--count");
        } else if (argument == "--batch-size" && index + 1 < argc) {
            options.batch_size =
                parse_positive_int(argv[++index], "--batch-size");
        } else if (argument == "--interval-ms" && index + 1 < argc) {
            options.interval_ms =
                parse_positive_int(argv[++index], "--interval-ms");
        } else if (argument == "--validate-payload") {
            options.validate_payload = true;
        } else if (argument == "--help") {
            std::cout
                << "Usage: eventhub_avro_publisher "
                   "[--count EVENTS] [--batch-size EVENTS] "
                   "[--interval-ms MILLISECONDS] "
                   "[--validate-payload]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument(
                "Unknown or incomplete argument: " + std::string(argument));
        }
    }
    return options;
}

const avro::ValidSchema& stock_tick_schema() {
    // Function-local static initialization is thread-safe in C++11 and later,
    // so the JSON schema is compiled only once for the process.
    static const avro::ValidSchema schema = [] {
        std::istringstream stream(kAvroSchema);
        avro::ValidSchema compiled;
        avro::compileJsonSchema(stream, compiled);
        return compiled;
    }();
    return schema;
}

std::vector<std::uint8_t> serialize_avro_container(
    const sample::StockTick& tick) {
    auto output = avro::memoryOutputStream();
    auto* output_view = output.get();

    // DataFileWriter creates a complete Avro Object Container File (OCF),
    // including the Obj\x01 header, writer schema, data block, and sync marker.
    avro::DataFileWriter<sample::StockTick> writer(
        std::move(output),
        stock_tick_schema());
    writer.write(tick);
    writer.flush();

    // Snapshot before close because the writer owns the output stream.
    auto payload = *avro::snapshot(*output_view);
    writer.close();

    return payload;
}

sample::StockTick deserialize_avro_container(
    const std::vector<std::uint8_t>& payload) {
    // OCF payloads start with the four-byte magic value "Obj" followed by 0x01.
    // A raw Avro datum has no such header and the direct Eventhouse connector
    // rejects it because it cannot discover the writer schema.
    constexpr std::array<std::uint8_t, 4> magic = {'O', 'b', 'j', 1};
    if (payload.size() < magic.size() ||
        !std::equal(magic.begin(), magic.end(), payload.begin())) {
        throw std::runtime_error(
            "Generated payload is not an Avro Object Container File");
    }

    auto input = avro::memoryInputStream(payload.data(), payload.size());
    avro::DataFileReader<sample::StockTick> reader(
        std::move(input),
        stock_tick_schema());

    sample::StockTick decoded;
    if (!reader.read(decoded)) {
        throw std::runtime_error("Generated Avro container has no record");
    }

    sample::StockTick unexpected;
    if (reader.read(unexpected)) {
        throw std::runtime_error(
            "Generated Avro container must contain exactly one record");
    }
    reader.close();
    return decoded;
}

void validate_round_trip(
    const sample::StockTick& expected,
    const std::vector<std::uint8_t>& payload) {
    // This is a publisher-side diagnostic, not part of normal delivery.
    // It catches schema/codec mistakes before the bytes leave the process.
    const auto decoded = deserialize_avro_container(payload);
    if (decoded.eventname != expected.eventname ||
        decoded.eventtime != expected.eventtime ||
        decoded.ticker != expected.ticker ||
        decoded.price != expected.price ||
        decoded.eventdesc != expected.eventdesc) {
        throw std::runtime_error(
            "Decoded Avro record does not match the generated stock tick");
    }
}

sample::StockTick next_tick(
    std::array<TickerState, 5>& tickers,
    std::mt19937_64& random) {
    // Apply a small random walk and constrain it to +/-10% of the starting
    // price so long tests continue to produce plausible demonstration data.
    std::uniform_int_distribution<std::size_t> choose_ticker(
        0,
        tickers.size() - 1);
    std::uniform_real_distribution<double> move(-0.002, 0.002);

    auto& ticker = tickers[choose_ticker(random)];
    ticker.price *= 1.0 + move(random);
    ticker.price = std::clamp(
        ticker.price,
        ticker.base_price * 0.90,
        ticker.base_price * 1.10);
    ticker.price = std::round(ticker.price * 100.0) / 100.0;

    const auto event_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    return sample::StockTick{
        .eventname = "stock ticks",
        .eventtime = event_time,
        .ticker = ticker.symbol,
        .price = ticker.price,
        .eventdesc = "stock ticker price",
    };
}

Azure::Messaging::EventHubs::Models::EventData make_event(
    const sample::StockTick& tick,
    std::vector<std::uint8_t> payload) {
    Azure::Messaging::EventHubs::Models::EventData event;
    // Event Hubs treats Body as opaque bytes; ContentType is descriptive
    // metadata and does not perform Avro serialization.
    event.Body = std::move(payload);
    event.ContentType = "avro/binary";

    // MessageId and application properties are AMQP metadata. They are useful
    // to consumers and diagnostics, but Eventhouse decodes Body according to
    // the data connection's Avro format and ingestion mapping.
    event.MessageId = Azure::Core::Amqp::Models::AmqpValue(
        tick.ticker + "-" + std::to_string(tick.eventtime));
    event.Properties["avro.schema.name"] =
        Azure::Core::Amqp::Models::AmqpValue(std::string(kSchemaName));
    return event;
}

std::pair<Azure::Messaging::EventHubs::Models::EventData, std::size_t>
prepare_event(
    const sample::StockTick& tick,
    bool validate_payload) {
    auto payload = serialize_avro_container(tick);
    const auto payload_size = payload.size();
    if (validate_payload) {
        validate_round_trip(tick, payload);
    }

    auto event = make_event(tick, std::move(payload));
    return {std::move(event), payload_size};
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const Options options = parse_options(argc, argv);
        const std::string fully_qualified_namespace =
            require_environment_variable("EVENTHUBS_HOST");
        const std::string event_hub_name =
            require_environment_variable("EVENTHUB_NAME");

        // DefaultAzureCredential uses the Azure CLI login during local
        // development and can use managed identity when hosted in Azure.
        // The selected identity needs Azure Event Hubs Data Sender permission.
        Azure::Messaging::EventHubs::ProducerClient producer(
            fully_qualified_namespace,
            event_hub_name,
            std::make_shared<Azure::Identity::DefaultAzureCredential>());

        std::array<TickerState, 5> tickers = {{
            {"AAPL", 230.00, 230.00},
            {"MSFT", 420.00, 420.00},
            {"NVDA", 180.00, 180.00},
            {"AMZN", 225.00, 225.00},
            {"GOOGL", 200.00, 200.00},
        }};
        std::mt19937_64 random(std::random_device{}());

        const int minimum_batch_count =
            (options.count + options.batch_size - 1) / options.batch_size;
        int sent_batch_count = 0;
        int sent_event_count = 0;
        auto batch = producer.CreateBatch();
        std::vector<PublishedEvent> batch_events;
        batch_events.reserve(static_cast<std::size_t>(options.batch_size));

        // Keep logging state beside the SDK batch. ProducerClient::Send sends
        // all EventData currently in the batch as one Event Hubs operation.
        const auto send_current_batch = [&] {
            producer.Send(batch);
            ++sent_batch_count;
            sent_event_count += static_cast<int>(batch_events.size());
            std::cout << "Sent batch " << sent_batch_count
                      << ": events=" << batch_events.size()
                      << ", totalEventsSent=" << sent_event_count << '\n';
            for (const auto& published : batch_events) {
                std::cout
                    << "  Avro OCF: eventname=\""
                    << published.tick.eventname
                    << "\", eventtime=" << published.tick.eventtime
                    << ", ticker=\"" << published.tick.ticker
                    << "\", price=" << std::fixed << std::setprecision(2)
                    << published.tick.price << ", eventdesc=\""
                    << published.tick.eventdesc
                    << "\", payloadBytes=" << published.payload_size
                    << ", magic=Obj\\x01, validation="
                    << (options.validate_payload ? "passed" : "not requested")
                    << '\n';
            }
            batch_events.clear();
        };

        std::cout << "Starting test: totalEvents=" << options.count
                  << ", maxEventsPerBatch=" << options.batch_size
                  << ", minimumBatches=" << minimum_batch_count
                  << ", validation="
                  << (options.validate_payload ? "enabled" : "disabled")
                  << '\n';

        for (int index = 0; index < options.count; ++index) {
            const auto tick = next_tick(tickers, random);
            auto [event, payload_size] =
                prepare_event(tick, options.validate_payload);

            // TryAdd checks the encoded AMQP size against the service limit.
            // A byte-full batch can therefore be sent before batch_size is
            // reached. The rejected event is then retried in a fresh batch.
            if (!batch.TryAdd(event)) {
                if (batch.NumberOfEvents() == 0) {
                    throw std::runtime_error(
                        "An Avro event exceeds the Event Hubs batch byte limit");
                }

                send_current_batch();

                std::this_thread::sleep_for(
                    std::chrono::milliseconds(options.interval_ms));

                batch = producer.CreateBatch();
                if (!batch.TryAdd(event)) {
                    throw std::runtime_error(
                        "An Avro event exceeds the Event Hubs batch byte limit");
                }
            }

            batch_events.push_back(PublishedEvent{tick, payload_size});

            const bool reached_event_limit =
                static_cast<int>(batch_events.size()) == options.batch_size;
            const bool is_last_event = index + 1 == options.count;
            if (!reached_event_limit && !is_last_event) {
                continue;
            }

            send_current_batch();

            if (!is_last_event) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(options.interval_ms));
                batch = producer.CreateBatch();
            }
        }

        std::cout << "Completed test: eventsSent=" << sent_event_count
                  << ", batchesSent=" << sent_batch_count << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Publisher failed: " << error.what() << '\n';
        return 1;
    }
}
