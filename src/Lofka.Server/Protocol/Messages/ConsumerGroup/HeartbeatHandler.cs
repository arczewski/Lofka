using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.ConsumerGroup;

public static class HeartbeatHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, ConsumerGroupManager groupManager)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 4;

        string groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        int generationId = reader.ReadInt32();
        string memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        // group_instance_id (v3+)
        if (header.ApiVersion >= 3)
        {
            if (isFlexible) reader.ReadCompactNullableString();
            else reader.ReadNullableString();
        }

        short errorCode = 0;
        var group = groupManager.GetGroup(groupId);
        if (group != null)
            errorCode = group.Heartbeat(memberId, generationId);
        else
            errorCode = 25; // UNKNOWN_MEMBER_ID

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        writer.WriteInt16(errorCode);

        if (isFlexible)
            writer.WriteEmptyTagBuffer();
    }
}
