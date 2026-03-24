using Xunit;
using Confluent.Kafka;
using Lofka.Tests.Integration.Infrastructure;

namespace Lofka.Tests.Integration;

public class ProduceTests : IAsyncLifetime
{
    private EmbeddedLofkaServer _server = null!;

    public async Task InitializeAsync()
    {
        _server = new EmbeddedLofkaServer();
        await _server.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _server.DisposeAsync();
    }

    [Fact]
    public async Task Producer_CanPublishSingleMessage()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _server.BootstrapServers,
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();
        var result = await producer.ProduceAsync("test-topic",
            new Message<string, string> { Key = "key1", Value = "hello" });

        Assert.Equal("test-topic", result.Topic);
        Assert.Equal(0, result.Partition.Value);
        Assert.True(result.Offset.Value >= 0);
    }

    [Fact]
    public async Task Producer_CanPublishMultipleMessages()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _server.BootstrapServers,
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        for (int i = 0; i < 10; i++)
        {
            var result = await producer.ProduceAsync("multi-topic",
                new Message<string, string> { Key = $"key-{i}", Value = $"value-{i}" });

            Assert.Equal("multi-topic", result.Topic);
            Assert.True(result.Offset.Value >= 0);
        }
    }

    [Fact]
    public async Task Producer_CanPublishNullKey()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _server.BootstrapServers,
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();
        var result = await producer.ProduceAsync("null-key-topic",
            new Message<Null, string> { Value = "hello-no-key" });

        Assert.Equal("null-key-topic", result.Topic);
        Assert.True(result.Offset.Value >= 0);
    }

    [Fact]
    public async Task Producer_CanPublishWithHeaders()
    {
        var config = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(config).Build();

        var msg = new Message<string, string>
        {
            Key = "hdr-key",
            Value = "hdr-value",
            Headers = new Headers
            {
                { "trace-id", System.Text.Encoding.UTF8.GetBytes("abc-123") },
                { "source", System.Text.Encoding.UTF8.GetBytes("test") },
            }
        };

        var result = await producer.ProduceAsync("header-topic", msg);
        Assert.True(result.Offset.Value >= 0);
    }

    [Fact]
    public async Task Producer_SequentialOffsets_AreMonotonic()
    {
        var config = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(config).Build();

        long prevOffset = -1;
        for (int i = 0; i < 5; i++)
        {
            var result = await producer.ProduceAsync("offset-test",
                new Message<string, string> { Key = $"k{i}", Value = $"v{i}" });
            Assert.True(result.Offset.Value > prevOffset);
            prevOffset = result.Offset.Value;
        }
    }
}
