# C++ Avro Publisher for Azure Event Hubs

This is a C++ **Event Hubs Avro publisher** sample that generates simulated stock-ticker events, serializes each event as an Apache Avro Object Container File, and publishes it to Azure Event Hubs using passwordless Microsoft Entra authentication. This is a sample and should be evaluated before using in production.

The sample intentionally keeps the implementation in one source file:
[`eventhub_avro_publisher.cpp`](eventhub_avro_publisher.cpp).

## Message format

Each Event Hubs message body is a complete Avro Object Container File containing
one `StockTick` record:

```json
{
  "type": "record",
  "name": "StockTick",
  "namespace": "sample",
  "fields": [
    {"name": "eventname", "type": "string"},
    {
      "name": "eventtime",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      }
    },
    {"name": "ticker", "type": "string"},
    {"name": "price", "type": "double"},
    {"name": "eventdesc", "type": "string"}
  ]
}
```

The body begins with the Avro container file signature `Obj\x01` and includes
the writer schema. This short signature helps software recognize the file
format before trying to read the rest.

For an Avro Object Container File, the first four bytes are:

```text
4F 62 6A 01
 O  b  j \x01
```

The AMQP message also contains:

```text
Content-Type: avro/binary
Message ID: <ticker>-<eventtime>
Application property: avro.schema.name=sample.StockTick
```

Azure Event Hubs stores and forwards the body as opaque bytes. A downstream
consumer must be configured to parse the body as Avro.

This sample uses AMQP endpoints, but Azure Event Hubs supports both AMQP endpoints and Kafka-compatible endpoints. They are two different ways of accessing the same Event Hub partitions and retained events.

### Field usage

| Field | Avro type | Purpose | Recommendation |
| --- | --- | --- | --- |
| `eventname` | `string` | Logical event category | Keep when several event types share a table; otherwise it is optional |
| `eventtime` | `long` / `timestamp-millis` | Source event time in Unix milliseconds | Required for time-series filtering and recommended over relying only on ingestion time |
| `ticker` | `string` | Stock symbol | Required business key for this sample |
| `price` | `double` | Simulated stock price | Required measure; add a currency field in a real multi-currency feed |
| `eventdesc` | `string` | Human-readable description | Optional; omit fixed descriptive text in a high-volume production feed |

The schema is valid and deliberately small. In a production contract, consider
adding an event ID for deduplication and a schema or contract version if those
values are needed independently of the Avro writer schema. Do not add fields
that are constant, unused, or already available from Event Hubs metadata.
For a new schema, consistent `camelCase` names are conventional; these lowercase
names are retained to match the existing `StockTicks` Eventhouse sample.

### Handling five fixed fields and dynamic fields

Avro isn't schema-less: every message still has a writer schema that defines
all its fields. Eventhouse can nevertheless keep a stable table contract when
the Avro records contain additional top-level fields. Store the five fields
used for filtering and aggregation as typed columns, and capture everything
else in a `dynamic` property bag:

```kusto
.create-merge table StockTicks (
    eventname: string,
    eventtime: datetime,
    ticker: string,
    price: real,
    eventdesc: string,
    properties: dynamic
)
```

Map the fixed fields normally, then map the complete record to `properties`
with `DropMappedFields`. The transform removes fields already mapped to typed
columns, leaving only the additional fields:

```kusto
.create-or-alter table StockTicks ingestion avro mapping
'StockTicksFlexibleAvroMapping'
'[{"column":"eventname","path":"$.eventname"},
  {"column":"eventtime","path":"$.eventtime","transform":"DateTimeFromUnixMilliseconds"},
  {"column":"ticker","path":"$.ticker"},
  {"column":"price","path":"$.price"},
  {"column":"eventdesc","path":"$.eventdesc"},
  {"column":"properties","path":"$","transform":"DropMappedFields"}]'
```

For example, a future Avro writer schema could add `exchange`, `currency`, or
`sourceSystem` without adding Eventhouse columns. Query those values with:

```kusto
StockTicks
| extend
    exchange = tostring(properties.exchange),
    currency = tostring(properties.currency)
```

If you control the producer, an even more governed design is to add one
explicit Avro `properties` map and place optional attributes inside it. Avro
map values must share one declared value schema, or use an explicit union of
allowed types. Promote frequently queried or strongly typed properties to
normal Eventhouse columns; keep sparse, changing attributes in `dynamic`.
Avoid automatically creating a new table column for every incoming field,
which leads to uncontrolled schema growth.

### Why use an Avro Object Container?

For the direct Fabric Eventhouse connection, each Event Hubs message
must contain a complete Avro Object Container File. A raw Avro binary datum was
rejected by this ingestion path because it has no `Obj\x01` header or embedded
writer schema.

Embedding the schema in every message adds overhead, so one-record containers
aren't the most space-efficient general-purpose Avro transport. They are used
here for compatibility with the direct Eventhouse `Avro` data format. For a
different consumer that supports schema registries or externally supplied
schemas, raw datum encoding may be more efficient.

### Serialization and deserialization

- **Serialization is required by the publisher.** It converts the in-memory
  `StockTick` C++ object into the Avro bytes sent in `EventData.Body`.
- **Deserialization is not required to publish.** This sample uses it only when
  `--validate-payload` is supplied. It reads the generated container back and
  verifies that it contains exactly one record matching the original object.
- Eventhouse performs the downstream deserialization using the Avro writer
  schema and the configured ingestion mapping.

## Prerequisites

- CMake 3.20 or later
- A C++20 compiler
- [vcpkg](https://github.com/microsoft/vcpkg)
- Azure CLI
- An Azure Event Hubs namespace and Event Hub
- `Azure Event Hubs Data Sender` assigned to your user or managed identity at
  the Event Hub or namespace scope

The vcpkg manifest restores:

- Apache Avro C++
- Azure Identity SDK for C++
- Azure Event Hubs SDK for C++
- fmt, used for an Avro C++ 1.12.1 header compatibility workaround

## Authenticate

For local development:

```powershell
az login --use-device-code --tenant <tenant-id>
az account set --subscription <subscription-id>
```

The program uses `DefaultAzureCredential`. In Azure-hosted environments, the
same code can use a managed identity with the `Azure Event Hubs Data Sender`
role.

## Build on Windows

Use a short build path to avoid Windows dependency path-length problems:

```powershell
$source = Get-Location
$vcpkgRoot = Join-Path $env:USERPROFILE "vcpkg"
$cmake = (Get-Command cmake -ErrorAction SilentlyContinue).Source

if (-not $cmake) {
  $cmake = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
}
if (-not (Test-Path $cmake)) {
  throw "CMake was not found. Install CMake or the Visual Studio C++ CMake tools."
}

& $cmake --version

& $cmake `
  -S $source `
  -B "C:\b\eventhub-avro" `
  -G "Visual Studio 17 2022" `
  -A x64 `
  -DCMAKE_TOOLCHAIN_FILE="$vcpkgRoot\scripts\buildsystems\vcpkg.cmake" `
  -DVCPKG_TARGET_TRIPLET=x64-windows `
  -DVCPKG_HOST_TRIPLET=x64-windows

& $cmake --build "C:\b\eventhub-avro" --config Release
```

If Visual Studio is installed elsewhere, locate its CMake executable with:

```powershell
Get-ChildItem "${env:ProgramFiles(x86)}\Microsoft Visual Studio" `
  -Filter cmake.exe -File -Recurse -ErrorAction SilentlyContinue |
  Select-Object -ExpandProperty FullName
```

## Build on Linux or macOS

```bash
cmake \
  -S . \
  -B build/eventhub-avro \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

cmake --build build/eventhub-avro
```

## Run

Set the Event Hubs namespace hostname and Event Hub name:

```powershell
$env:EVENTHUBS_HOST = "<namespace>.servicebus.windows.net"
$env:EVENTHUB_NAME = "<event-hub-name>"

& "C:\b\eventhub-avro\Release\eventhub_avro_publisher.exe" `
  --count 25 `
  --batch-size 10 `
  --interval-ms 100 `
  --validate-payload
```

Arguments:

| Argument | Default | Purpose |
| --- | ---: | --- |
| `--count` | `10` | Total number of Event Hubs messages to publish |
| `--batch-size` | `1` | Maximum EventData messages to add to each Event Hubs batch |
| `--interval-ms` | `100` | Delay between batch sends |
| `--validate-payload` | Off | Deserialize each generated OCF locally and verify a one-record round trip before publishing |
| `--help` | | Display usage |

The requested minimum batch count is:

```text
ceiling(count / batch-size)
```

For example, `--count 25 --batch-size 10` sends batches containing 10, 10,
and 5 messages. The actual count can be higher if the SDK's maximum batch byte
size is reached before the requested message count.

Expected output:

```text
Starting test: totalEvents=25, maxEventsPerBatch=10, minimumBatches=3, validation=enabled
Sent batch 1: events=10, totalEventsSent=10
  Avro OCF: eventname="stock ticks", eventtime=1789650000000, ticker="MSFT", price=420.25, eventdesc="stock ticker price", payloadBytes=406, magic=Obj\x01, validation=passed
...
Sent batch 3: events=5, totalEventsSent=25
Completed test: eventsSent=25, batchesSent=3
```

No connection strings or access keys are required by the publisher.

## Recommended sending method

For this direct Eventhouse ingestion scenario:

1. Serialize each logical stock tick as its own complete Avro OCF body.
2. Create an Event Hubs `EventDataBatch`.
3. Add each `EventData` with `TryAdd`, which enforces the service's batch byte
   limit.
4. Send the batch using the long-lived `ProducerClient`.

An Event Hubs batch is a transport optimization: it sends several independent
EventData messages in one service operation. It doesn't combine all records
into one EventData body. This preserves per-message metadata and lets
Eventhouse decode each stock tick independently.

The fixed `--batch-size` makes tests deterministic. In a production
throughput-oriented publisher, it is common to keep adding messages until
`TryAdd` returns false, send the full batch, and then continue with a new
batch. Production code should also define retry, cancellation, idempotency,
and failed-message handling behavior.

This demo namespace uses the Standard tier, where the maximum
publication size is 1 MB for either one event or an entire batch. A good
starting target is 500-800 KB per batch, leaving room for AMQP metadata and
per-message overhead rather than aiming exactly at 1 MB. The current Avro
messages are approximately 406 bytes each, so start with 500 messages per
batch (about 203 KB of body data) or increase toward 1,000 messages (about
406 KB plus AMQP overhead) while monitoring latency and throughput. Check the
limits for the tier used by your own namespace.

## Configure a Fabric Eventhouse destination

These steps configure a direct Azure Event Hubs data connection. A Fabric
Eventstream isn't required.

### 1. Create a dedicated consumer group

Use one consumer group per downstream application so Eventhouse doesn't
compete with another receiver for partition ownership:

```powershell
az eventhubs eventhub consumer-group create `
  --subscription <subscription-id> `
  --resource-group <resource-group> `
  --namespace-name <namespace-name> `
  --eventhub-name <event-hub-name> `
  --name fabric-eventhouse
```

The publisher doesn't select a consumer group. Consumer groups are selected
only by receivers such as the Eventhouse data connection.

### 2. Create the Eventhouse table and Avro mapping

Open the target KQL database query window and run:

```kusto
.create-merge table StockTicks (
    eventname: string,
    eventtime: datetime,
    ticker: string,
    price: real,
    eventdesc: string
)
```

Then create the mapping:

```kusto
.create-or-alter table StockTicks ingestion avro mapping
'StockTicksAvroMapping'
'[{"column":"eventname","path":"$.eventname"},
  {"column":"eventtime","path":"$.eventtime","transform":"DateTimeFromUnixMilliseconds"},
  {"column":"ticker","path":"$.ticker"},
  {"column":"price","path":"$.price"},
  {"column":"eventdesc","path":"$.eventdesc"}]'
```

An ingestion mapping is a named set of rules that tells Eventhouse which Avro
field populates each table column. Each entry identifies the destination
`column`, the source field `path`, and, when needed, an ingest-time
`transform`. The Event Hub data connection references this mapping for every
message it ingests.

The `DateTimeFromUnixMilliseconds` transform converts the Avro logical
timestamp value into the Eventhouse `datetime` column. Without an explicit
mapping, Eventhouse uses case-sensitive identity mapping, which is suitable
only when source field names and types already match the table exactly.

### 3. Create the direct data connection

In Fabric:

1. Open the Eventhouse and select the target KQL database.
2. Select **Get data** or **Data connections**, then create an
   **Azure Event Hubs** connection.
3. Select or create a cloud connection for the Event Hubs namespace.
4. Select the Event Hub and the dedicated `fabric-eventhouse` consumer group.
5. Configure:

   | Setting | Value |
   | --- | --- |
   | Data format | `Avro` |
   | Target table | `StockTicks` |
   | Mapping | `StockTicksAvroMapping` |
   | Compression | `None` |

6. Create the connection and confirm that its status is active.

Some Fabric UI versions don't expose `Avro` in the format dropdown even though
the underlying direct Kusto/Eventhouse data connection supports
`DataFormat=Avro`. If it isn't listed, create the connection through the
supported data-connection API or automation and set the table, mapping,
consumer group, and `Avro` data format explicitly.

Use the connector's supported passwordless identity option where available. If
the connector requires shared-access authentication, create a dedicated
authorization rule with **Listen only** permission. Never reuse a
Manage/Send-capable key. Azure Policy may disable local/SAS authentication; use
an approved narrowly scoped exemption only when required by the connector and
your organization's security policy.

Raw Avro datum bytes without the `Obj\x01` container header aren't accepted by
the direct Eventhouse Avro ingestion path. The complete object container
created by this sample is required.

## Recommended test sequence

Test in layers so a failure can be isolated quickly.

### 1. Check the executable and local Avro round trip

```powershell
& "C:\b\eventhub-avro\Release\eventhub_avro_publisher.exe" --help

& "C:\b\eventhub-avro\Release\eventhub_avro_publisher.exe" `
  --count 5 `
  --batch-size 2 `
  --interval-ms 100 `
  --validate-payload
```

`validation=passed` proves that the body has the Avro OCF magic bytes, contains
exactly one record, and deserializes to the original field values. It doesn't
by itself prove Event Hubs delivery or Eventhouse ingestion. This command
should report three batch sends: 2, 2, and 1 message.

### 2. Confirm Event Hubs accepted the send

The process must exit with code `0`, print `Sent batch`, and list each
`Avro OCF` event. For repeatable
integration testing, publish a small count such as 1-5 rather than a long
continuous stream. Azure Event Hubs metrics can also confirm incoming
messages, but they don't prove that Eventhouse decoded them.

### 3. Verify the decoded Eventhouse row

After publishing, allow for ingestion latency and run:

```kusto
StockTicks
| where eventtime > ago(10m)
| project eventtime, ticker, price, eventname, eventdesc
| order by eventtime desc
| take 100
```

The row should have a source `eventtime` close to the publish time and values
matching the console output.

### 4. Diagnose ingestion failures

If no row appears, check:

```kusto
.show ingestion failures
| where FailedOn > ago(1h)
| project FailedOn, Table, ErrorCode, Details
| order by FailedOn desc
```

A `wrong magic in header` failure means the message contains raw Avro datum
bytes rather than a complete Avro Object Container. Also verify that the
connection uses the expected consumer group, table, mapping, and `Avro` data
format.

## Production considerations

- Batch multiple `EventData` messages for higher throughput. Start with a
  moderate `--batch-size`, measure latency and throughput, and tune it for the
  workload.
- Reuse one `ProducerClient`, as this sample does.
- Use a dedicated consumer group per downstream application.
- Keep credentials outside source code.
- Add retry, cancellation, structured logging, and monitoring appropriate for
  your hosting environment.
- Review Event Hubs throughput units, retention, partitioning, and message-size
  limits before production use.
