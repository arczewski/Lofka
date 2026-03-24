using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Headers;

public static class ResponseHeader
{
    /// <summary>
    /// Writes response header v0 (correlation_id only).
    /// </summary>
    public static void WriteV0(BigEndianWriter writer, int correlationId)
    {
        writer.WriteInt32(correlationId);
    }

    /// <summary>
    /// Writes response header v1 (correlation_id + empty tag buffer).
    /// </summary>
    public static void WriteV1(BigEndianWriter writer, int correlationId)
    {
        writer.WriteInt32(correlationId);
        writer.WriteEmptyTagBuffer();
    }

    /// <summary>
    /// Writes the appropriate response header based on whether the API version is flexible.
    /// Special case: ApiVersions always uses v0 header.
    /// </summary>
    public static void Write(BigEndianWriter writer, int correlationId, short apiKey, short apiVersion)
    {
        // ApiVersions (key 18) always uses response header v0
        if (apiKey == 18)
        {
            WriteV0(writer, correlationId);
            return;
        }

        if (ApiRegistry.IsFlexibleVersion(apiKey, apiVersion))
        {
            WriteV1(writer, correlationId);
        }
        else
        {
            WriteV0(writer, correlationId);
        }
    }
}
