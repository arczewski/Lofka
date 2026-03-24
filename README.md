# Lofka

> **Highly experimental. This project was vibe-coded with Claude Code — use at your own risk.**

A lightweight, Kafka-compatible broker for development environments. Single binary, Native AOT compiled, ~15MB container image, millisecond startup.

Lofka implements enough of the Kafka wire protocol for standard .NET Kafka clients (Confluent.Kafka / librdkafka) to produce, consume, use consumer groups, and manage topics — without the JVM, ZooKeeper, or KRaft overhead.

## Features

- **Kafka wire protocol** — binary-compatible with Confluent.Kafka / librdkafka
- **Produce & consume** — including consumer groups with automatic partition assignment
- **Topic management** — auto-create on first use, or create/delete via AdminClient
- **Offset management** — commit and fetch consumer offsets
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

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `9092` | Listen port |
| `--host` | `localhost` | Advertised hostname |
| `--partitions` | `1` | Default partition count for auto-created topics |
| `--no-auto-create` | | Disable automatic topic creation |

```bash
docker run -p 19092:19092 ghcr.io/arczewski/lofka:latest --port 19092 --partitions 3
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
