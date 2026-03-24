using Lofka.Server;
using Lofka.Server.Configuration;
using Lofka.Server.Network;

var config = ServerConfig.FromArgs(args);
using var server = new LofkaServer(config);

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

await server.StartAsync(cts.Token);

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
}

LofkaLogger.Info("Lofka shutting down...");
