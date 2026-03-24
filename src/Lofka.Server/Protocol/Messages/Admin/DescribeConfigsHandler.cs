using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class DescribeConfigsHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body, BigEndianWriter writer)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 4;

        // Parse resources array
        int resourceCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var resources = new List<(byte ResourceType, string ResourceName)>();

        for (int i = 0; i < resourceCount; i++)
        {
            byte resourceType = reader.ReadInt8();
            string resourceName = isFlexible ? reader.ReadCompactString() : reader.ReadString();

            // config_names (nullable array of strings)
            int configNameCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            if (configNameCount > 0)
            {
                for (int c = 0; c < configNameCount; c++)
                {
                    if (isFlexible) reader.ReadCompactString();
                    else reader.ReadString();
                    if (isFlexible) reader.SkipTagBuffer();
                }
            }

            if (isFlexible) reader.SkipTagBuffer();
            resources.Add((resourceType, resourceName));
        }

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms
        writer.WriteInt32(0);

        // results array
        if (isFlexible)
        {
            writer.WriteCompactArrayLength(resources.Count);
            foreach (var (resourceType, resourceName) in resources)
            {
                writer.WriteInt16(0); // error_code
                writer.WriteCompactNullableString(null); // error_message
                writer.WriteInt8(resourceType);
                writer.WriteCompactString(resourceName);
                writer.WriteCompactArrayLength(0); // configs (empty)
                writer.WriteEmptyTagBuffer();
            }
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(resources.Count);
            foreach (var (resourceType, resourceName) in resources)
            {
                writer.WriteInt16(0); // error_code
                writer.WriteNullableString(null); // error_message
                writer.WriteInt8(resourceType);
                writer.WriteString(resourceName);
                writer.WriteArrayLength(0); // configs (empty)
            }
        }
    }
}
