# C++ Avro Publisher for Azure Event Hubs

This C++ application generates simulated stock-ticker events, serializes each
event as an Apache Avro Object Container File, and publishes it to Azure Event
Hubs using Microsoft Entra authentication.

The implementation is contained in one source file:
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

The Azure Event Hubs C++ SDK sends these messages over AMQP. Event Hubs also
provides a Kafka-compatible endpoint over the same partitions and retained
events. In Kafka terms, the Avro body is the record value and AMQP application
properties correspond to Kafka record headers.

### Field usage

| Field | Avro type | Purpose | Implementation |
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

Avro is not schema-less: every message still has a writer schema that defines
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

An Avro writer schema can add `exchange`, `currency`, or `sourceSystem`
without adding Eventhouse columns. Query those values with:

```kusto
StockTicks
| extend
    exchange = tostring(properties.exchange),
    currency = tostring(properties.currency)
```

For producer-controlled schemas, define an explicit Avro `properties` map and
place optional attributes inside it. Avro map values share one declared value
schema or an explicit union of allowed types. Frequently queried or strongly
typed properties belong in normal Eventhouse columns; sparse and changing
attributes belong in `dynamic`. This prevents a new table column from being
created for every incoming field.

### Why use an Avro Object Container?

For the direct Fabric Eventhouse connection, each Event Hubs message
must contain a complete Avro Object Container File. A raw Avro binary datum is
incompatible because it has no `Obj\x01` header or embedded
writer schema.

Embedding the schema in every message adds overhead, so one-record containers
use more space than raw Avro datums. This implementation uses containers
because the direct Eventhouse `Avro` data format requires the container header
and embedded writer schema.

### Serialization and deserialization

- **Serialization is required by the publisher.** It converts the in-memory
  `StockTick` C++ object into the Avro bytes sent in `EventData.Body`.
- **Deserialization is not required to publish.** The application uses it when
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
- `Azure Event Hubs Data Sender` assigned to the user or managed identity at
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

## Batching behavior

The publisher sends data as follows:

1. Serialize each logical stock tick as its own complete Avro OCF body.
2. Create an Event Hubs `EventDataBatch`.
3. Add each `EventData` with `TryAdd`, which enforces the service's batch byte
   limit.
4. Send the batch using the long-lived `ProducerClient`.

An Event Hubs batch is a transport optimization: it sends several independent
EventData messages in one service operation. It does not combine all records
into one EventData body. This preserves per-message metadata and lets
Eventhouse decode each stock tick independently.

`--batch-size` sets the maximum message count per send. `TryAdd` also checks
the encoded AMQP size against the tier-specific Event Hubs publication limit.
When the byte limit is reached first, the application sends the current batch,
creates a new batch, and retries the rejected event.

## Configure a Fabric Eventhouse destination

These steps configure a direct Azure Event Hubs data connection. A Fabric
Eventstream is not required.

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

### 1. Create a dedicated consumer group

Use one consumer group per downstream application so Eventhouse does not
compete with another receiver for partition ownership:

```powershell
az eventhubs eventhub consumer-group create `
  --subscription <subscription-id> `
  --resource-group <resource-group> `
  --namespace-name <namespace-name> `
  --eventhub-name <event-hub-name> `
  --name fabric-eventhouse
```

The publisher does not select a consumer group. Consumer groups are selected
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

### 3. Create the table and mapping through the Kusto API

Set the deployment values:

```powershell
$subscriptionId = "<subscription-id>"
$resourceGroup = "<resource-group>"
$namespaceName = "<event-hubs-namespace>"
$eventHubName = "<event-hub-name>"
$consumerGroup = "fabric-eventhouse"
$sasRuleName = "FabricEventhouseListen"

$workspaceId = "<fabric-workspace-id>"
$databaseId = "<kql-database-item-id>"
$databaseName = "<kql-database-name>"
$capacityId = "<fabric-capacity-id>"
$queryServiceUri = "https://<cluster>.kusto.fabric.microsoft.com"
```

Create the table and named Avro mapping through the Kusto management endpoint:

```powershell
$tableCommand = @'
.create-merge table StockTicks (
    eventname: string,
    eventtime: datetime,
    ticker: string,
    price: real,
    eventdesc: string
)
'@

$mappingCommand = @'
.create-or-alter table StockTicks ingestion avro mapping
'StockTicksAvroMapping'
'[{"column":"eventname","path":"$.eventname"},
  {"column":"eventtime","path":"$.eventtime","transform":"DateTimeFromUnixMilliseconds"},
  {"column":"ticker","path":"$.ticker"},
  {"column":"price","path":"$.price"},
  {"column":"eventdesc","path":"$.eventdesc"}]'
'@

foreach ($command in @($tableCommand, $mappingCommand)) {
  $bodyPath = Join-Path $env:TEMP "eventhouse-management.json"
  @{
    db = $databaseName
    csl = $command
  } |
    ConvertTo-Json -Compress |
    Set-Content -Path $bodyPath -Encoding utf8NoBOM

  az rest `
    --method post `
    --resource "https://kusto.kusto.windows.net" `
    --url "$queryServiceUri/v1/rest/mgmt" `
    --headers "Content-Type=application/json" `
    --body "@$bodyPath"
}
```

`StockTicksAvroMapping` maps the Avro fields to the Eventhouse columns. The
`DateTimeFromUnixMilliseconds` transform converts the Avro
`timestamp-millis` value to an Eventhouse `datetime`.

### 4. Create the Fabric Event Hub cloud connection

Create a dedicated listen-only Event Hubs authorization rule:

```powershell
az eventhubs eventhub authorization-rule create `
  --subscription $subscriptionId `
  --resource-group $resourceGroup `
  --namespace-name $namespaceName `
  --eventhub-name $eventHubName `
  --name $sasRuleName `
  --rights Listen

$sasKey = az eventhubs eventhub authorization-rule keys list `
  --subscription $subscriptionId `
  --resource-group $resourceGroup `
  --namespace-name $namespaceName `
  --eventhub-name $eventHubName `
  --name $sasRuleName `
  --query primaryKey `
  --output tsv
```

Create the Fabric cloud connection:

```powershell
$connectionBodyPath = Join-Path $env:TEMP "eventhub-cloud-connection.json"
@{
  connectivityType = "ShareableCloud"
  displayName = "<fabric-event-hub-connection-name>"
  connectionDetails = @{
    type = "EventHub"
    creationMethod = "EventHub.Contents"
    parameters = @(
      @{
        dataType = "Text"
        name = "endpoint"
        value = "$namespaceName.servicebus.windows.net"
      },
      @{
        dataType = "Text"
        name = "entityPath"
        value = $eventHubName
      }
    )
  }
  privacyLevel = "Organizational"
  credentialDetails = @{
    singleSignOnType = "None"
    connectionEncryption = "NotEncrypted"
    skipTestConnection = $false
    credentials = @{
      credentialType = "Basic"
      username = $sasRuleName
      password = $sasKey
    }
  }
} |
  ConvertTo-Json -Depth 10 |
  Set-Content -Path $connectionBodyPath -Encoding utf8NoBOM

$cloudConnection = az rest `
  --method post `
  --resource "https://api.fabric.microsoft.com" `
  --url "https://api.fabric.microsoft.com/v1/connections" `
  --headers "Content-Type=application/json" `
  --body "@$connectionBodyPath" `
  --output json |
    ConvertFrom-Json

$cloudConnectionId = $cloudConnection.id
```

The SAS key remains in process memory and is stored in the Fabric connection.
It is not added to source control or application configuration.

### 5. Create the direct Avro data connection

Request a Kusto workload token:

```powershell
$mwcTokenBodyPath = Join-Path $env:TEMP "kusto-mwc-token.json"
@{
  type = "[Start] GetMWCTokenV2"
  workloadType = "Kusto"
  artifactObjectIds = @($databaseId)
  workspaceObjectId = $workspaceId
  capacityObjectId = $capacityId
} |
  ConvertTo-Json -Depth 5 |
  Set-Content -Path $mwcTokenBodyPath -Encoding utf8NoBOM

$mwcTokenResponse = az rest `
  --method post `
  --resource "https://analysis.windows.net/powerbi/api" `
  --url "https://wabi-us-central-b-primary-redirect.analysis.windows.net/metadata/v201606/generatemwctokenv2" `
  --headers "Content-Type=application/json" `
  --body "@$mwcTokenBodyPath" `
  --output json |
    ConvertFrom-Json

$mwcToken = $mwcTokenResponse.Token
```

## Verify the implementation

Verify serialization, delivery, and ingestion separately.

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
exactly one record, and deserializes to the original field values. It does not
by itself prove Event Hubs delivery or Eventhouse ingestion. This command
should report three batch sends: 2, 2, and 1 message.

### 2. Confirm Event Hubs accepted the send

The process exits with code `0`, prints `Sent batch`, and lists each
`Avro OCF` event. Event Hubs incoming-message metrics confirm delivery to the
service but do not confirm Eventhouse decoding.

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

## Implementation boundaries

- The application batches multiple `EventData` messages and reuses one
  `ProducerClient`.
- Use a dedicated consumer group per downstream application.
- Credentials remain outside source code through `DefaultAzureCredential`.
- Host-level retry, cancellation, structured logging, and monitoring are not
  implemented.
- Event Hubs throughput units, retention, partition count, and tier limits are
  deployment configuration rather than application configuration.

## References

- [Create a Fabric connection](https://learn.microsoft.com/rest/api/fabric/core/connections/create-connection)
- [Create an ingestion mapping](https://learn.microsoft.com/kusto/management/create-ingestion-mapping-command)
- [Ingest data from Event Hubs](https://learn.microsoft.com/azure/data-explorer/ingest-data-event-hub-overview)
- [Supported ingestion formats](https://learn.microsoft.com/azure/data-explorer/ingestion-supported-formats)
