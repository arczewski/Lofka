namespace Lofka.Server;

public static class LofkaLogger
{
    private static readonly Dictionary<short, string> ApiNames = new()
    {
        [0] = "Produce",
        [1] = "Fetch",
        [2] = "ListOffsets",
        [3] = "Metadata",
        [8] = "OffsetCommit",
        [9] = "OffsetFetch",
        [10] = "FindCoordinator",
        [11] = "JoinGroup",
        [12] = "Heartbeat",
        [13] = "LeaveGroup",
        [14] = "SyncGroup",
        [18] = "ApiVersions",
        [19] = "CreateTopics",
        [20] = "DeleteTopics",
        [22] = "InitProducerId",
        [32] = "DescribeConfigs",
    };

    public static string GetApiName(short apiKey)
    {
        return ApiNames.TryGetValue(apiKey, out var name) ? name : $"Unknown({apiKey})";
    }

    public static void Info(string message)
    {
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] [INF] {message}");
    }

    public static void Warn(string message)
    {
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] [WRN] {message}");
    }

    public static void Error(string message)
    {
        Console.Error.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] [ERR] {message}");
    }

    public static void Request(short apiKey, short apiVersion, int correlationId, string? clientId)
    {
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] [REQ] {GetApiName(apiKey)} v{apiVersion} corr={correlationId} client={clientId ?? "-"}");
    }

    public static void Response(short apiKey, int correlationId, int responseBytes)
    {
        Console.WriteLine($"[{DateTime.UtcNow:HH:mm:ss.fff}] [RSP] {GetApiName(apiKey)} corr={correlationId} bytes={responseBytes}");
    }
}
