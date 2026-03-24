using Lofka.Server.Configuration;
using Lofka.Server.Network;

namespace Lofka.Tests.Integration.Infrastructure;

public sealed class EmbeddedLofkaServer : IAsyncDisposable
{
    private readonly LofkaServer _server;
    private readonly ServerConfig _config;

    public EmbeddedLofkaServer(int port = 0, int defaultPartitions = 1)
    {
        _config = new ServerConfig
        {
            Port = port,
            AdvertisedHost = "localhost",
            DefaultPartitionCount = defaultPartitions,
            AutoCreateTopics = true,
        };
        _server = new LofkaServer(_config);
    }

    public int Port => _server.Port;
    public string BootstrapServers => $"localhost:{Port}";

    public async Task StartAsync()
    {
        await _server.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _server.Dispose();
        // Give connections time to close
        await Task.Delay(50);
    }
}
