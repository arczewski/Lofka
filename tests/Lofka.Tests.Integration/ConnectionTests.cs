using Xunit;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Lofka.Tests.Integration.Infrastructure;

namespace Lofka.Tests.Integration;

public class ConnectionTests : IAsyncLifetime
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
    public void Client_CanConnect_AndGetMetadata()
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = _server.BootstrapServers,
        };

        using var adminClient = new AdminClientBuilder(config).Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));

        Assert.NotNull(metadata);
        Assert.Single(metadata.Brokers);
        Assert.Equal(_server.Port, metadata.Brokers[0].Port);
    }

    [Fact]
    public void Client_CanGetMetadata_ForSpecificTopic()
    {
        var config = new AdminClientConfig
        {
            BootstrapServers = _server.BootstrapServers,
        };

        using var adminClient = new AdminClientBuilder(config).Build();
        var metadata = adminClient.GetMetadata("auto-created-topic", TimeSpan.FromSeconds(5));

        Assert.NotNull(metadata);
        Assert.Single(metadata.Topics);
        Assert.Equal("auto-created-topic", metadata.Topics[0].Topic);
        Assert.True(metadata.Topics[0].Partitions.Count > 0);
    }
}
