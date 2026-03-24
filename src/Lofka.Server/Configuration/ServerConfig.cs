namespace Lofka.Server.Configuration;

public sealed class ServerConfig
{
    public int Port { get; set; } = 9092;
    public string AdvertisedHost { get; set; } = "localhost";
    public int DefaultPartitionCount { get; set; } = 1;
    public bool AutoCreateTopics { get; set; } = true;
    public int MaxLogBytesPerPartition { get; set; } = 100 * 1024 * 1024; // 100MB

    public static ServerConfig FromArgs(string[] args)
    {
        var config = new ServerConfig();

        // Environment variables (lowest priority — args override)
        if (Environment.GetEnvironmentVariable("LOFKA_ADVERTISED_HOST") is { Length: > 0 } envHost)
            config.AdvertisedHost = envHost;
        else
            config.AdvertisedHost = Environment.MachineName; // container hostname by default

        if (Environment.GetEnvironmentVariable("LOFKA_PORT") is { Length: > 0 } envPort)
            config.Port = int.Parse(envPort);

        if (Environment.GetEnvironmentVariable("LOFKA_PARTITIONS") is { Length: > 0 } envPart)
            config.DefaultPartitionCount = int.Parse(envPart);

        if (Environment.GetEnvironmentVariable("LOFKA_AUTO_CREATE_TOPICS") is "false" or "0")
            config.AutoCreateTopics = false;

        // CLI args (highest priority)
        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--port" when i + 1 < args.Length:
                    config.Port = int.Parse(args[++i]);
                    break;
                case "--host" when i + 1 < args.Length:
                    config.AdvertisedHost = args[++i];
                    break;
                case "--partitions" when i + 1 < args.Length:
                    config.DefaultPartitionCount = int.Parse(args[++i]);
                    break;
                case "--no-auto-create":
                    config.AutoCreateTopics = false;
                    break;
                case "--healthcheck":
                    Environment.Exit(0);
                    break;
            }
        }

        return config;
    }
}
