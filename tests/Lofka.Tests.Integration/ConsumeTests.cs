using Xunit;
using Confluent.Kafka;
using Lofka.Tests.Integration.Infrastructure;

namespace Lofka.Tests.Integration;

public class ConsumeTests : IAsyncLifetime
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
    public async Task Consumer_CanReadProducedMessage_ManualAssign()
    {
        // Produce
        var pConfig = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(pConfig).Build();
        await producer.ProduceAsync("consume-test",
            new Message<string, string> { Key = "k1", Value = "v1" });

        // Consume with manual partition assignment
        var cConfig = new ConsumerConfig
        {
            BootstrapServers = _server.BootstrapServers,
            GroupId = "unused-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        using var consumer = new ConsumerBuilder<string, string>(cConfig).Build();
        consumer.Assign(new TopicPartitionOffset("consume-test", 0, Offset.Beginning));

        var result = consumer.Consume(TimeSpan.FromSeconds(10));

        Assert.NotNull(result);
        Assert.Equal("k1", result.Message.Key);
        Assert.Equal("v1", result.Message.Value);
    }

    [Fact]
    public async Task Consumer_CanReadMultipleMessages_ManualAssign()
    {
        // Produce multiple messages
        var pConfig = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(pConfig).Build();

        for (int i = 0; i < 5; i++)
        {
            await producer.ProduceAsync("multi-consume-test",
                new Message<string, string> { Key = $"k-{i}", Value = $"v-{i}" });
        }

        // Consume
        var cConfig = new ConsumerConfig
        {
            BootstrapServers = _server.BootstrapServers,
            GroupId = "unused-group-2",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        using var consumer = new ConsumerBuilder<string, string>(cConfig).Build();
        consumer.Assign(new TopicPartitionOffset("multi-consume-test", 0, Offset.Beginning));

        var messages = new List<ConsumeResult<string, string>>();
        for (int i = 0; i < 5; i++)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(10));
            if (result != null) messages.Add(result);
        }

        Assert.Equal(5, messages.Count);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal($"k-{i}", messages[i].Message.Key);
            Assert.Equal($"v-{i}", messages[i].Message.Value);
        }
    }
}
