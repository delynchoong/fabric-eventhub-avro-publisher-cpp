#include <azure/identity.hpp>
#include <azure/messaging/eventhubs.hpp>

#include <fmt/format.h>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Decoder.hh>
#include <avro/Encoder.hh>
#include <avro/Specific.hh>
#include <avro/Stream.hh>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

// Primary implementation for the validated Fabric Eventhouse pattern. Each
// EventData body is a complete self-describing Avro OCF whose top-level record
// contains fixed primitive fields and a variablefields map. Raw Avro datum and
// Fabric cross-database routing are intentionally excluded because the PoC
// proved those paths do not work with the direct Eventhouse connection.
namespace sample {

using MapValue =
    std::variant<std::monostate, std::string, bool, std::int64_t, double>;

struct DynamicMapTick {
    std::string eventname;
    std::int64_t eventtime;
    std::string ticker;
    double price;
    std::string eventdesc;
    std::map<std::string, MapValue> variablefields;
};

}  // namespace sample

namespace {

void encode_map_value(
    avro::Encoder& encoder,
    const sample::MapValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        encoder.encodeUnionIndex(0);
        encoder.encodeNull();
    } else if (const auto* text = std::get_if<std::string>(&value)) {
        encoder.encodeUnionIndex(1);
        avro::encode(encoder, *text);
    } else if (const auto* flag = std::get_if<bool>(&value)) {
        encoder.encodeUnionIndex(2);
        avro::encode(encoder, *flag);
    } else if (const auto* number = std::get_if<std::int64_t>(&value)) {
        encoder.encodeUnionIndex(3);
        avro::encode(encoder, *number);
    } else {
        encoder.encodeUnionIndex(4);
        avro::encode(encoder, std::get<double>(value));
    }
}

sample::MapValue decode_map_value(avro::Decoder& decoder) {
    switch (decoder.decodeUnionIndex()) {
        case 0:
            decoder.decodeNull();
            return std::monostate{};
        case 1: {
            std::string value;
            avro::decode(decoder, value);
            return value;
        }
        case 2: {
            bool value = false;
            avro::decode(decoder, value);
            return value;
        }
        case 3: {
            std::int64_t value = 0;
            avro::decode(decoder, value);
            return value;
        }
        case 4: {
            double value = 0.0;
            avro::decode(decoder, value);
            return value;
        }
        default:
            throw std::runtime_error(
                "Unexpected Avro union branch for a map value");
    }
}

}  // namespace

namespace avro {

template <>
struct codec_traits<sample::DynamicMapTick> {
    static void encode(
        Encoder& encoder,
        const sample::DynamicMapTick& value) {
        avro::encode(encoder, value.eventname);
        avro::encode(encoder, value.eventtime);
        avro::encode(encoder, value.ticker);
        avro::encode(encoder, value.price);
        avro::encode(encoder, value.eventdesc);

        encoder.mapStart();
        if (!value.variablefields.empty()) {
            encoder.setItemCount(value.variablefields.size());
            for (const auto& [key, map_value] : value.variablefields) {
                encoder.startItem();
                avro::encode(encoder, key);
                encode_map_value(encoder, map_value);
            }
        }
        encoder.mapEnd();
    }

    static void decode(
        Decoder& decoder,
        sample::DynamicMapTick& value) {
        avro::decode(decoder, value.eventname);
        avro::decode(decoder, value.eventtime);
        avro::decode(decoder, value.ticker);
        avro::decode(decoder, value.price);
        avro::decode(decoder, value.eventdesc);

        value.variablefields.clear();
        for (std::size_t count = decoder.mapStart();
             count != 0;
             count = decoder.mapNext()) {
            for (std::size_t index = 0; index < count; ++index) {
                std::string key;
                avro::decode(decoder, key);
                value.variablefields.emplace(
                    std::move(key), decode_map_value(decoder));
            }
        }
    }
};

}  // namespace avro

namespace {

constexpr const char* kAvroSchema = R"({
  "type": "record",
  "name": "DynamicMapTick",
  "namespace": "sample",
  "fields": [
    {"name": "eventname", "type": "string"},
    {"name": "eventtime", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "ticker", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "eventdesc", "type": "string"},
    {
      "name": "variablefields",
      "type": {
        "type": "map",
        "values": ["null", "string", "boolean", "long", "double"]
      },
      "default": {}
    }
  ]
})";

struct Options {
    bool print_message = false;
    bool validate_only = false;
    std::string dump_avro;
};

Options parse_options(int argc, char* argv[]) {
    Options options;
    for (int index = 1; index < argc; ++index) {
        const std::string_view argument(argv[index]);
        if (argument == "--print-message") {
            options.print_message = true;
        } else if (argument == "--validate-only") {
            options.validate_only = true;
        } else if (argument == "--dump-avro" && index + 1 < argc) {
            options.dump_avro = argv[++index];
        } else if (argument == "--help") {
            std::cout
                << "Usage: eventhub_avro_map_producer "
                   "[--print-message] [--dump-avro PATH] "
                   "[--validate-only]\n";
            std::exit(0);
        } else {
            throw std::invalid_argument(
                "Unknown or incomplete argument: " + std::string(argument));
        }
    }
    return options;
}

std::string require_environment_variable(const char* name) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        throw std::runtime_error(std::string(name) + " is required");
    }
    return value;
}

const avro::ValidSchema& dynamic_map_schema() {
    static const avro::ValidSchema schema = [] {
        std::istringstream stream(kAvroSchema);
        avro::ValidSchema compiled;
        avro::compileJsonSchema(stream, compiled);
        return compiled;
    }();
    return schema;
}

std::vector<sample::DynamicMapTick> make_ticks() {
    const auto event_time =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count();

    return {
        {
            "dynamic Avro map ticks",
            event_time,
            "MAP1",
            101.25,
            "Avro map record with 1 variable key",
            {{"venue", std::string("LSE")}},
        },
        {
            "dynamic Avro map ticks",
            event_time,
            "MAP2",
            202.50,
            "Avro map record with 2 variable keys",
            {
                {"bid", 202.45},
                {"isIndicative", false},
            },
        },
        {
            "dynamic Avro map ticks",
            event_time,
            "MAP3",
            303.75,
            "Avro map record with 3 variable keys",
            {
                {"currency", std::string("GBP")},
                {"tradePrice", 303.75},
                {"tradeSize", std::int64_t{25000}},
            },
        },
        {
            "dynamic Avro map ticks",
            event_time,
            "MAP4",
            404.00,
            "Avro map record with 4 variable keys",
            {
                {"auctionType", std::string("Closing")},
                {"imbalance", 1250.5},
                {"isClosingAuction", true},
                {"matchedVolume", std::int64_t{900000}},
            },
        },
        {
            "dynamic Avro map ticks",
            event_time,
            "MAP5",
            505.25,
            "Avro map record with 5 variable keys",
            {
                {"condition", std::string("AT")},
                {"isCorrection", false},
                {"note", std::monostate{}},
                {"sequenceNumber", std::int64_t{987654321}},
                {"yield", 4.125},
            },
        },
    };
}

std::vector<std::uint8_t> serialize_ticks(
    const std::vector<sample::DynamicMapTick>& ticks) {
    if (ticks.empty()) {
        throw std::invalid_argument("ticks must not be empty");
    }

    auto output = avro::memoryOutputStream();
    auto* output_view = output.get();
    avro::DataFileWriter<sample::DynamicMapTick> writer(
        std::move(output), dynamic_map_schema());
    for (const auto& tick : ticks) {
        writer.write(tick);
        writer.flush();
    }
    auto payload = *avro::snapshot(*output_view);
    writer.close();
    return payload;
}

std::vector<sample::DynamicMapTick> deserialize_ticks(
    const std::vector<std::uint8_t>& payload) {
    auto input = avro::memoryInputStream(payload.data(), payload.size());
    avro::DataFileReader<sample::DynamicMapTick> reader(
        std::move(input), dynamic_map_schema());

    std::vector<sample::DynamicMapTick> ticks;
    sample::DynamicMapTick tick;
    while (reader.read(tick)) {
        ticks.push_back(std::move(tick));
        tick = {};
    }
    reader.close();
    return ticks;
}

std::int64_t decode_avro_long(
    const std::vector<std::uint8_t>& payload,
    std::size_t& offset) {
    std::uint64_t encoded = 0;
    for (unsigned int byte_index = 0; byte_index < 10; ++byte_index) {
        if (offset >= payload.size()) {
            throw std::invalid_argument(
                "Avro OCF ended while decoding a long");
        }
        const std::uint8_t byte = payload[offset++];
        if (byte_index == 9 && (byte & 0xfeU) != 0) {
            throw std::invalid_argument("Avro OCF long is out of range");
        }
        encoded |= static_cast<std::uint64_t>(byte & 0x7fU)
                   << (byte_index * 7);
        if ((byte & 0x80U) == 0) {
            return static_cast<std::int64_t>(
                (encoded >> 1) ^
                (0U - static_cast<std::uint64_t>(encoded & 1U)));
        }
    }
    throw std::invalid_argument("Avro OCF long is out of range");
}

std::size_t checked_size(
    std::int64_t value,
    std::string_view description) {
    if (value < 0 ||
        static_cast<std::uint64_t>(value) >
            std::numeric_limits<std::size_t>::max()) {
        throw std::invalid_argument(
            "Invalid " + std::string(description) + " in Avro OCF");
    }
    return static_cast<std::size_t>(value);
}

void skip_bytes(
    const std::vector<std::uint8_t>& payload,
    std::size_t& offset,
    std::size_t size,
    std::string_view description) {
    if (offset > payload.size() || size > payload.size() - offset) {
        throw std::invalid_argument(
            "Avro OCF ended while reading " + std::string(description));
    }
    offset += size;
}

void skip_length_prefixed_bytes(
    const std::vector<std::uint8_t>& payload,
    std::size_t& offset,
    std::string_view description) {
    const auto size = checked_size(
        decode_avro_long(payload, offset), description);
    skip_bytes(payload, offset, size, description);
}

std::size_t count_data_blocks(
    const std::vector<std::uint8_t>& payload) {
    if (payload.size() < 4 ||
        payload[0] != 'O' || payload[1] != 'b' ||
        payload[2] != 'j' || payload[3] != 1) {
        throw std::invalid_argument(
            "payload is not an Avro object container file");
    }

    std::size_t offset = 4;
    while (true) {
        const std::int64_t item_count =
            decode_avro_long(payload, offset);
        if (item_count == 0) {
            break;
        }
        if (item_count < 0) {
            if (item_count == std::numeric_limits<std::int64_t>::min()) {
                throw std::invalid_argument(
                    "Avro OCF metadata item count is out of range");
            }
            const auto block_size = checked_size(
                decode_avro_long(payload, offset),
                "metadata block size");
            skip_bytes(
                payload, offset, block_size, "metadata block");
            continue;
        }
        for (std::int64_t index = 0; index < item_count; ++index) {
            skip_length_prefixed_bytes(
                payload, offset, "metadata key");
            skip_length_prefixed_bytes(
                payload, offset, "metadata value");
        }
    }

    std::array<std::uint8_t, 16> sync_marker{};
    if (offset > payload.size() ||
        sync_marker.size() > payload.size() - offset) {
        throw std::invalid_argument(
            "Avro OCF header is missing its sync marker");
    }
    std::copy_n(
        payload.begin() + static_cast<std::ptrdiff_t>(offset),
        sync_marker.size(),
        sync_marker.begin());
    offset += sync_marker.size();

    std::size_t block_count = 0;
    while (offset < payload.size()) {
        if (decode_avro_long(payload, offset) <= 0) {
            throw std::invalid_argument(
                "Avro OCF data block must contain records");
        }
        const auto block_size = checked_size(
            decode_avro_long(payload, offset), "data block size");
        skip_bytes(payload, offset, block_size, "data block");
        if (offset > payload.size() ||
            sync_marker.size() > payload.size() - offset ||
            !std::equal(
                sync_marker.begin(),
                sync_marker.end(),
                payload.begin() + static_cast<std::ptrdiff_t>(offset))) {
            throw std::invalid_argument(
                "Avro OCF data block has an invalid sync marker");
        }
        offset += sync_marker.size();
        ++block_count;
    }
    return block_count;
}

void validate_round_trip(
    const std::vector<sample::DynamicMapTick>& expected,
    const std::vector<std::uint8_t>& payload) {
    const auto actual = deserialize_ticks(payload);
    if (actual.size() != expected.size()) {
        throw std::runtime_error(
            "Decoded Avro map record count changed");
    }
    for (std::size_t index = 0; index < expected.size(); ++index) {
        const auto& expected_tick = expected[index];
        const auto& actual_tick = actual[index];
        if (actual_tick.eventname != expected_tick.eventname ||
            actual_tick.eventtime != expected_tick.eventtime ||
            actual_tick.ticker != expected_tick.ticker ||
            actual_tick.price != expected_tick.price ||
            actual_tick.eventdesc != expected_tick.eventdesc ||
            actual_tick.variablefields != expected_tick.variablefields) {
            throw std::runtime_error(
                "Decoded Avro map record differs from its source");
        }
    }
    if (count_data_blocks(payload) != expected.size()) {
        throw std::runtime_error(
            "Avro OCF does not contain one block per map record");
    }
}

void append_json_value(
    std::ostream& output,
    const sample::MapValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        output << "null";
    } else if (const auto* text = std::get_if<std::string>(&value)) {
        output << std::quoted(*text);
    } else if (const auto* flag = std::get_if<bool>(&value)) {
        output << (*flag ? "true" : "false");
    } else if (const auto* number = std::get_if<std::int64_t>(&value)) {
        output << *number;
    } else {
        output << std::setprecision(15) << std::get<double>(value);
    }
}

void print_records(
    const std::vector<sample::DynamicMapTick>& ticks,
    const std::vector<std::uint8_t>& payload) {
    for (std::size_t index = 0; index < ticks.size(); ++index) {
        const auto& tick = ticks[index];
        std::cout << "Avro map record " << index + 1 << ": {"
                  << "\"eventname\":" << std::quoted(tick.eventname)
                  << ",\"eventtime\":" << tick.eventtime
                  << ",\"ticker\":" << std::quoted(tick.ticker)
                  << ",\"price\":" << std::setprecision(15) << tick.price
                  << ",\"eventdesc\":" << std::quoted(tick.eventdesc)
                  << ",\"variablefields\":{";
        std::size_t field_index = 0;
        for (const auto& [key, value] : tick.variablefields) {
            if (field_index++ != 0) {
                std::cout << ',';
            }
            std::cout << std::quoted(key) << ':';
            append_json_value(std::cout, value);
        }
        std::cout << "}}\n";
    }
    std::cout << "Avro OCF body: records=" << ticks.size()
              << ", dataBlocks=" << count_data_blocks(payload)
              << ", bytes=" << payload.size()
              << ", magic=4f 62 6a 01 (Obj\\x01)\n";
}

void dump_payload(
    const std::string& path,
    const std::vector<std::uint8_t>& payload) {
    std::ofstream output(path, std::ios::binary);
    if (!output) {
        throw std::runtime_error(
            "Unable to create Avro dump file: " + path);
    }
    output.write(
        reinterpret_cast<const char*>(payload.data()),
        static_cast<std::streamsize>(payload.size()));
    output.close();
    if (!output) {
        throw std::runtime_error(
            "Unable to write Avro dump file: " + path);
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    try {
        const Options options = parse_options(argc, argv);
        const auto ticks = make_ticks();
        const auto payload = serialize_ticks(ticks);
        validate_round_trip(ticks, payload);

        if (options.print_message) {
            print_records(ticks, payload);
        }
        if (!options.dump_avro.empty()) {
            dump_payload(options.dump_avro, payload);
            std::cout << "Wrote exact EventData.Body to "
                      << options.dump_avro << '\n';
        }
        if (options.validate_only) {
            std::cout << "Validated " << ticks.size()
                      << " Avro map records in "
                      << count_data_blocks(payload)
                      << " data blocks without publishing\n";
            return 0;
        }

        Azure::Messaging::EventHubs::ProducerClient producer(
            require_environment_variable("EVENTHUBS_HOST"),
            require_environment_variable("EVENTHUB_NAME"),
            std::make_shared<Azure::Identity::DefaultAzureCredential>());

        Azure::Messaging::EventHubs::Models::EventData event;
        event.Body = payload;
        event.ContentType = "avro/binary";
        event.MessageId = Azure::Core::Amqp::Models::AmqpValue(
            "dynamic-map-" + std::to_string(ticks.front().eventtime));
        event.Properties["Table"] =
            Azure::Core::Amqp::Models::AmqpValue("DynamicTicks");
        event.Properties["Format"] =
            Azure::Core::Amqp::Models::AmqpValue("Avro");
        event.Properties["IngestionMappingReference"] =
            Azure::Core::Amqp::Models::AmqpValue(
                "DynamicTicksAvroMapping");
        event.Properties["Compression"] =
            Azure::Core::Amqp::Models::AmqpValue("None");
        event.Properties["avro.schema.name"] =
            Azure::Core::Amqp::Models::AmqpValue(
                "sample.DynamicMapTick");

        auto batch = producer.CreateBatch();
        if (!batch.TryAdd(event)) {
            throw std::runtime_error(
                "The Avro map EventData exceeds the batch size");
        }
        producer.Send(batch);

        std::cout << "Sent one EventData message containing "
                  << ticks.size() << " Avro records in "
                  << count_data_blocks(payload)
                  << " data blocks; local validation passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Producer failed: " << error.what() << '\n';
        return 1;
    }
}
