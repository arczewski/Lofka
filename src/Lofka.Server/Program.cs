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
Console.WriteLine($"Lofka listening on port {config.Port}");

try
{
    await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
}

Console.WriteLine("Lofka shutting down...");
