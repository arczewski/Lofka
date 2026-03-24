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

    [Fact]
    public async Task Consumer_CanReadMessageWithHeaders()
    {
        var pConfig = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(pConfig).Build();

        var msg = new Message<string, string>
        {
            Key = "hk",
            Value = "hv",
            Headers = new Headers
            {
                { "trace-id", System.Text.Encoding.UTF8.GetBytes("xyz-789") },
            }
        };
        await producer.ProduceAsync("header-consume-test", msg);

        var cConfig = new ConsumerConfig
        {
            BootstrapServers = _server.BootstrapServers,
            GroupId = "hdr-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        using var consumer = new ConsumerBuilder<string, string>(cConfig).Build();
        consumer.Assign(new TopicPartitionOffset("header-consume-test", 0, Offset.Beginning));

        var result = consumer.Consume(TimeSpan.FromSeconds(10));

        Assert.NotNull(result);
        Assert.Equal("hk", result.Message.Key);
        Assert.Equal("hv", result.Message.Value);
        Assert.Single(result.Message.Headers);
        Assert.Equal("trace-id", result.Message.Headers[0].Key);
        Assert.Equal("xyz-789", System.Text.Encoding.UTF8.GetString(result.Message.Headers[0].GetValueBytes()));
    }

    [Fact]
    public async Task Consumer_ReceivesEmpty_WhenNoMessages()
    {
        var pConfig = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(pConfig).Build();
        // Create the topic via a produce
        await producer.ProduceAsync("empty-consume-test",
            new Message<string, string> { Key = "x", Value = "x" });

        var cConfig = new ConsumerConfig
        {
            BootstrapServers = _server.BootstrapServers,
            GroupId = "empty-group",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false,
        };
        using var consumer = new ConsumerBuilder<string, string>(cConfig).Build();
        consumer.Assign(new TopicPartitionOffset("empty-consume-test", 0, Offset.End));

        // No new messages — should return null within timeout
        var result = consumer.Consume(TimeSpan.FromSeconds(2));
        Assert.Null(result);
    }
}
