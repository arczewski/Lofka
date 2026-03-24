using Lofka.Server.Configuration;
using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Messages.FindCoordinator;

public static class FindCoordinatorHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body, BigEndianWriter writer,
        ServerConfig config)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 3;

        // v0-v2: key (string), v3: key_type, coordinator_keys (array)
        if (header.ApiVersion >= 3)
        {
            // key_type
            reader.ReadInt8();
            // coordinator_keys (compact string array)
            int keyCount = reader.ReadCompactArrayLength();
            for (int i = 0; i < keyCount; i++)
                reader.ReadCompactString();
            reader.SkipTagBuffer();
        }
        else
        {
            reader.ReadString(); // key
            if (header.ApiVersion >= 1)
                reader.ReadInt8(); // key_type
        }

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        if (header.ApiVersion >= 3)
        {
            // v3+: array of coordinators
            writer.WriteCompactArrayLength(1);
            writer.WriteCompactString(""); // key
            writer.WriteInt32(0); // node_id
            writer.WriteCompactString(config.AdvertisedHost);
            writer.WriteInt32(config.Port);
            writer.WriteInt16(0); // error_code
            writer.WriteCompactNullableString(null); // error_message
            writer.WriteEmptyTagBuffer(); // coordinator tags
            writer.WriteEmptyTagBuffer(); // top-level tags
        }
        else
        {
            // v0-v2: single coordinator
            writer.WriteInt16(0); // error_code
            if (header.ApiVersion >= 1)
                writer.WriteNullableString(null); // error_message
            writer.WriteInt32(0); // node_id
            writer.WriteString(config.AdvertisedHost);
            writer.WriteInt32(config.Port);
        }
    }
}
