using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Messages.ApiVersions;

public static class ApiVersionsHandler
{
    public static void Handle(RequestHeader header, BigEndianWriter writer)
    {
        // ApiVersions ALWAYS uses response header v0 (no tag buffer)
        ResponseHeader.WriteV0(writer, header.CorrelationId);

        var apis = ApiRegistry.GetSupportedApis();

        // error_code
        writer.WriteInt16(0);

        if (header.ApiVersion >= 3)
        {
            // Flexible version: compact array
            writer.WriteCompactArrayLength(apis.Length);
            foreach (var api in apis)
            {
                writer.WriteInt16(api.ApiKey);
                writer.WriteInt16(api.MinVersion);
                writer.WriteInt16(api.MaxVersion);
                writer.WriteEmptyTagBuffer(); // per-api tags
            }
            // throttle_time_ms
            writer.WriteInt32(0);
            writer.WriteEmptyTagBuffer(); // top-level tags
        }
        else
        {
            // Non-flexible version: standard array
            writer.WriteArrayLength(apis.Length);
            foreach (var api in apis)
            {
                writer.WriteInt16(api.ApiKey);
                writer.WriteInt16(api.MinVersion);
                writer.WriteInt16(api.MaxVersion);
            }
            // throttle_time_ms (v1+)
            if (header.ApiVersion >= 1)
                writer.WriteInt32(0);
        }
    }
}
