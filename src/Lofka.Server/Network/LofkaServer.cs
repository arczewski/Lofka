using System.Net;
using System.Net.Sockets;
using Lofka.Server.Configuration;
using Lofka.Server.Protocol;
using Lofka.Server.Storage;

namespace Lofka.Server.Network;

public sealed class LofkaServer : IDisposable
{
    private readonly ServerConfig _config;
    private readonly TcpListener _listener;
    private readonly TopicStore _topicStore;
    private readonly ConsumerGroupManager _groupManager;
    private readonly OffsetStore _offsetStore;
    private readonly RequestDispatcher _dispatcher;
    private CancellationTokenSource? _cts;
    private Task? _acceptTask;
    private long _nextProducerId;

    public LofkaServer(ServerConfig config)
    {
        _config = config;
        _listener = new TcpListener(IPAddress.IPv6Any, config.Port);
        _listener.Server.DualMode = true;
        _topicStore = new TopicStore(config);
        _groupManager = new ConsumerGroupManager();
        _offsetStore = new OffsetStore();
        _dispatcher = new RequestDispatcher(config, _topicStore, _groupManager, _offsetStore, this);
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;
    public ServerConfig Config => _config;

    public long GetNextProducerId() => Interlocked.Increment(ref _nextProducerId);

    public Task StartAsync(CancellationToken cancellationToken = default)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _listener.Start();

        // If port was 0, update config with the actual port
        if (_config.Port == 0)
            _config.Port = ((IPEndPoint)_listener.LocalEndpoint).Port;

        _acceptTask = AcceptLoopAsync(_cts.Token);
        return Task.CompletedTask;
    }

    private async Task AcceptLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var tcpClient = await _listener.AcceptTcpClientAsync(ct);
                tcpClient.NoDelay = true;
                var connection = new ClientConnection(tcpClient, _dispatcher);
                _ = connection.RunAsync(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ObjectDisposedException)
            {
                break;
            }
        }
    }

    public void Dispose()
    {
        _cts?.Cancel();
        _listener.Stop();
        _cts?.Dispose();
    }
}
