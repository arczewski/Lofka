namespace Lofka.Server.Protocol;

/// <summary>
/// Registry of supported Kafka API keys, version ranges, and flexible version boundaries.
/// </summary>
public static class ApiRegistry
{
    public record struct ApiVersionRange(short ApiKey, short MinVersion, short MaxVersion, short FlexibleSince);

    // Flexible version = -1 means no flexible versions supported for that API
    private static readonly ApiVersionRange[] SupportedApis =
    [
        new(0,  3, 7,  9),   // Produce (flexible since v9, we cap at v7)
        new(1,  4, 11, 12),  // Fetch (flexible since v12, we cap at v11)
        new(2,  1, 5,  6),   // ListOffsets (flexible since v6, we cap at v5)
        new(3,  1, 9,  9),   // Metadata (flexible since v9)
        new(8,  2, 7,  8),   // OffsetCommit (flexible since v8, we cap at v7)
        new(9,  1, 6,  6),   // OffsetFetch (flexible since v6)
        new(10, 0, 3,  3),   // FindCoordinator (flexible since v3)
        new(11, 0, 6,  6),   // JoinGroup (flexible since v6)
        new(12, 0, 3,  4),   // Heartbeat (flexible since v4, we cap at v3)
        new(13, 0, 3,  4),   // LeaveGroup (flexible since v4, we cap at v3)
        new(14, 0, 3,  4),   // SyncGroup (flexible since v4, we cap at v3)
        new(18, 0, 3,  3),   // ApiVersions (flexible since v3)
        new(19, 0, 5,  5),   // CreateTopics (flexible since v5)
        new(20, 0, 4,  4),   // DeleteTopics (flexible since v4)
        new(22, 0, 2,  2),   // InitProducerId (flexible since v2, we cap at v2)
    ];

    /// <summary>Returns all supported API version ranges.</summary>
    public static ReadOnlySpan<ApiVersionRange> GetSupportedApis() => SupportedApis;

    /// <summary>Checks if the given API key + version uses the flexible message format.</summary>
    public static bool IsFlexibleVersion(short apiKey, short apiVersion)
    {
        foreach (var api in SupportedApis)
        {
            if (api.ApiKey == apiKey)
            {
                return api.FlexibleSince >= 0 && apiVersion >= api.FlexibleSince;
            }
        }
        return false;
    }

    /// <summary>Checks if the given API key is supported.</summary>
    public static bool IsSupported(short apiKey)
    {
        foreach (var api in SupportedApis)
        {
            if (api.ApiKey == apiKey) return true;
        }
        return false;
    }
}
