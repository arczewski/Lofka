using Lofka.Server.Configuration;
using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class DescribeClusterHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, ServerConfig config)
    {
        // DescribeCluster is always flexible (introduced at v0 as flexible)
        ResponseHeader.WriteV1(writer, header.CorrelationId);

        writer.WriteInt32(0); // throttle_time_ms
        writer.WriteInt16(0); // error_code
        writer.WriteCompactNullableString(null); // error_message
        writer.WriteCompactString("lofka-cluster"); // cluster_id
        writer.WriteInt32(0); // controller_id
        // brokers array
        writer.WriteCompactArrayLength(1);
        writer.WriteInt32(0); // broker_id
        writer.WriteCompactString(config.AdvertisedHost);
        writer.WriteInt32(config.Port);
        writer.WriteCompactNullableString(null); // rack
        writer.WriteEmptyTagBuffer(); // broker tags
        writer.WriteInt32(-2147483648); // cluster_authorized_operations
        writer.WriteEmptyTagBuffer(); // top-level tags
    }
}
