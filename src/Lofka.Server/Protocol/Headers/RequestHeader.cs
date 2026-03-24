using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Headers;

public sealed class RequestHeader
{
    public short ApiKey { get; init; }
    public short ApiVersion { get; init; }
    public int CorrelationId { get; init; }
    public string? ClientId { get; init; }

    /// <summary>
    /// Parses a request header from the payload buffer.
    /// Returns the header and the offset where the body starts.
    /// </summary>
    public static (RequestHeader Header, int BodyOffset) Parse(ReadOnlySpan<byte> buffer)
    {
        var reader = new BigEndianReader(buffer);

        short apiKey = reader.ReadInt16();
        short apiVersion = reader.ReadInt16();
        int correlationId = reader.ReadInt32();
        string? clientId = reader.ReadNullableString();

        // If this is a flexible version, skip the tag buffer in the header
        if (ApiRegistry.IsFlexibleVersion(apiKey, apiVersion))
        {
            reader.SkipTagBuffer();
        }

        return (new RequestHeader
        {
            ApiKey = apiKey,
            ApiVersion = apiVersion,
            CorrelationId = correlationId,
            ClientId = clientId,
        }, reader.Offset);
    }
}
