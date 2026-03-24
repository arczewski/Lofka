using Xunit;
using Confluent.Kafka;
using Lofka.Tests.Integration.Infrastructure;

namespace Lofka.Tests.Integration;

public class ConsumerGroupTests : IAsyncLifetime
{
    private EmbeddedLofkaServer _server = null!;

    public async Task InitializeAsync()
    {
        _server = new EmbeddedLofkaServer(defaultPartitions: 3);
        await _server.StartAsync();
    }

    public async Task DisposeAsync()
    {
        await _server.DisposeAsync();
    }

    [Fact]
    public async Task ConsumerGroup_SingleConsumer_GetsAllPartitions()
    {
        // Produce messages
        var pConfig = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(pConfig).Build();

        for (int i = 0; i < 9; i++)
        {
            await producer.ProduceAsync("group-test",
                new Message<string, string> { Key = $"key-{i}", Value = $"val-{i}" });
        }

        // Consume with Subscribe (consumer group)
        var cConfig = new ConsumerConfig
        {
            BootstrapServers = _server.BootstrapServers,
            GroupId = "test-group-1",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        using var consumer = new ConsumerBuilder<string, string>(cConfig).Build();
        consumer.Subscribe("group-test");

        var messages = new List<ConsumeResult<string, string>>();
        var deadline = DateTime.UtcNow.AddSeconds(30);

        while (messages.Count < 9 && DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(5));
            if (result != null) messages.Add(result);
        }

        Assert.Equal(9, messages.Count);
    }

    [Fact]
    public async Task ConsumerGroup_CanCommitOffsets()
    {
        // Produce a message
        var pConfig = new ProducerConfig { BootstrapServers = _server.BootstrapServers };
        using var producer = new ProducerBuilder<string, string>(pConfig).Build();
        await producer.ProduceAsync("commit-test",
            new Message<string, string> { Key = "k", Value = "v" });

        // Consume and commit
        var cConfig = new ConsumerConfig
        {
            BootstrapServers = _server.BootstrapServers,
            GroupId = "commit-group",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };
        using var consumer = new ConsumerBuilder<string, string>(cConfig).Build();
        consumer.Subscribe("commit-test");

        var result = consumer.Consume(TimeSpan.FromSeconds(15));
        Assert.NotNull(result);

        // Should not throw
        consumer.Commit(result);
    }
}
