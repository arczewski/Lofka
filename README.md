# Lofka

> **Highly experimental. This project was vibe-coded with Claude Code — use at your own risk.**

A lightweight, Kafka-compatible broker for development environments. Single binary, Native AOT compiled, ~15MB container image, millisecond startup.

Lofka implements enough of the Kafka wire protocol for standard .NET Kafka clients (Confluent.Kafka / librdkafka) to produce, consume, use consumer groups, and manage topics — without the JVM, ZooKeeper, or KRaft overhead.

## Features

- **Kafka wire protocol** — binary-compatible with Confluent.Kafka / librdkafka and Java Kafka clients
- **Produce & consume** — including consumer groups with automatic partition assignment
- **Topic management** — auto-create on first use, or create/delete via AdminClient
- **Offset management** — commit and fetch consumer offsets
- **Config management** — DescribeConfigs returns sensible defaults for topic and broker configs
- **Request logging** — full visibility into every request/response with timestamps and client IDs
- **Native AOT** — single binary, no .NET runtime required, instant startup
- **In-memory storage** — zero disk I/O, zero configuration
- **Dual-stack networking** — IPv4 and IPv6

### Supported Kafka APIs

| API | Status |
|-----|--------|
| ApiVersions | Supported |
| Metadata | Supported |
| Produce | Supported |
| Fetch (with long-poll) | Supported |
| ListOffsets | Supported |
| FindCoordinator | Supported |
| JoinGroup / SyncGroup / Heartbeat / LeaveGroup | Supported |
| OffsetCommit / OffsetFetch | Supported |
| CreateTopics / DeleteTopics | Supported |
| InitProducerId | Supported |
| DescribeConfigs | Supported |
| DescribeCluster | Supported |
| ListGroups / DescribeGroups | Supported |
| Transactions, ACLs, SASL/TLS | Not supported |

## Quick start

### Docker

```bash
docker pull ghcr.io/arczewski/lofka:latest
docker run -p 9092:9092 ghcr.io/arczewski/lofka:latest
```

### From source

```bash
dotnet run --project src/Lofka.Server
```

### Use with Confluent.Kafka

```csharp
var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
using var producer = new ProducerBuilder<string, string>(config).Build();

await producer.ProduceAsync("my-topic",
    new Message<string, string> { Key = "key", Value = "hello from lofka" });
```

```csharp
var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "my-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
};
using var consumer = new ConsumerBuilder<string, string>(config).Build();
consumer.Subscribe("my-topic");

var result = consumer.Consume(TimeSpan.FromSeconds(5));
Console.WriteLine($"{result.Message.Key}: {result.Message.Value}");
```

## Configuration

All settings can be set via environment variables or CLI flags. CLI flags take priority.

| Env Variable | CLI Flag | Default | Description |
|-------------|----------|---------|-------------|
| `LOFKA_ADVERTISED_HOST` | `--host` | container hostname | Hostname returned in Metadata responses — clients connect to this |
| `LOFKA_PORT` | `--port` | `9092` | Listen port |
| `LOFKA_PARTITIONS` | `--partitions` | `1` | Default partition count for auto-created topics |
| `LOFKA_AUTO_CREATE_TOPICS=false` | `--no-auto-create` | `true` | Disable automatic topic creation |

> **Docker networking tip:** By default, Lofka advertises the container hostname (e.g. `lofka`).
> When running with Docker Compose, name your service `lofka` and other containers on the same
> network will resolve it automatically. Override with `LOFKA_ADVERTISED_HOST` if needed.

### Docker Compose example

```yaml
services:
  lofka:
    image: ghcr.io/arczewski/lofka:latest
    ports:
      - "9092:9092"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: lofka:9092
```

## Building

### Prerequisites

- .NET 10 SDK

### Build & test

```bash
dotnet build
dotnet test
```

### Docker build

```bash
docker build -t lofka .
```

### Native AOT publish

```bash
dotnet publish src/Lofka.Server -c Release -r linux-x64 -o dist/
# produces a single ~10MB binary at dist/Lofka.Server
```

## Limitations

Lofka is **highly experimental** and was vibe-coded with AI assistance. It is designed for development and testing only — **not** suitable for production use.

- All data is stored in memory and lost on restart
- Single-node only — no replication, no fault tolerance
- No transactions, ACLs, quotas, or SASL/TLS
- No message compression handling (batches are stored opaquely)
- No log compaction or retention policies beyond memory limits

## License

MIT

---

*Apache Kafka is a registered trademark of the Apache Software Foundation. Lofka is an independent project and is not affiliated with or endorsed by the Apache Software Foundation.*
