using Xunit;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Lofka.Tests.Integration.Infrastructure;

namespace Lofka.Tests.Integration;

public class AdminTests : IAsyncLifetime
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
    public async Task AdminClient_CanCreateTopic()
    {
        var config = new AdminClientConfig { BootstrapServers = _server.BootstrapServers };
        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = "admin-topic", NumPartitions = 3, ReplicationFactor = 1 }
        });

        var metadata = adminClient.GetMetadata("admin-topic", TimeSpan.FromSeconds(5));
        Assert.Single(metadata.Topics);
        Assert.Equal("admin-topic", metadata.Topics[0].Topic);
        Assert.Equal(3, metadata.Topics[0].Partitions.Count);
    }

    [Fact]
    public async Task AdminClient_CanDeleteTopic()
    {
        var config = new AdminClientConfig { BootstrapServers = _server.BootstrapServers };
        using var adminClient = new AdminClientBuilder(config).Build();

        // Create first
        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = "delete-me", NumPartitions = 1, ReplicationFactor = 1 }
        });

        // Delete
        await adminClient.DeleteTopicsAsync(new[] { "delete-me" });

        // Verify — topic should no longer appear in metadata (all topics)
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
        Assert.DoesNotContain(metadata.Topics, t => t.Topic == "delete-me");
    }

    [Fact]
    public async Task AdminClient_CanCreateTopicWithMultiplePartitions()
    {
        var config = new AdminClientConfig { BootstrapServers = _server.BootstrapServers };
        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = "multi-part-topic", NumPartitions = 8, ReplicationFactor = 1 }
        });

        var metadata = adminClient.GetMetadata("multi-part-topic", TimeSpan.FromSeconds(5));
        Assert.Equal(8, metadata.Topics[0].Partitions.Count);
    }

    [Fact]
    public async Task AdminClient_CreateDuplicateTopic_ReturnsError()
    {
        var config = new AdminClientConfig { BootstrapServers = _server.BootstrapServers };
        using var adminClient = new AdminClientBuilder(config).Build();

        await adminClient.CreateTopicsAsync(new[]
        {
            new TopicSpecification { Name = "dup-topic", NumPartitions = 1, ReplicationFactor = 1 }
        });

        var ex = await Assert.ThrowsAsync<CreateTopicsException>(() =>
            adminClient.CreateTopicsAsync(new[]
            {
                new TopicSpecification { Name = "dup-topic", NumPartitions = 1, ReplicationFactor = 1 }
            }));

        Assert.Contains(ex.Results, r => r.Error.Code == ErrorCode.TopicAlreadyExists);
    }

    [Fact]
    public async Task AdminClient_DescribeConfigs_DoesNotThrow()
    {
        var config = new AdminClientConfig { BootstrapServers = _server.BootstrapServers };
        using var adminClient = new AdminClientBuilder(config).Build();

        // DescribeConfigs for broker 0 — should return empty configs without error
        var results = await adminClient.DescribeConfigsAsync(new[]
        {
            new ConfigResource { Type = ResourceType.Broker, Name = "0" }
        });

        Assert.Single(results);
    }
}
