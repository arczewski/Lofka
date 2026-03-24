using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.ConsumerGroup;

public static class LeaveGroupHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, ConsumerGroupManager groupManager)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 4;

        string groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        if (header.ApiVersion >= 3)
        {
            // v3+: members array
            int memberCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            var group = groupManager.GetGroup(groupId);
            for (int i = 0; i < memberCount; i++)
            {
                string memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
                if (isFlexible) reader.ReadCompactNullableString(); // group_instance_id
                else reader.ReadNullableString();
                if (isFlexible) reader.SkipTagBuffer();
                group?.Leave(memberId);
            }
        }
        else
        {
            string memberId = reader.ReadString();
            groupManager.GetGroup(groupId)?.Leave(memberId);
        }

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        writer.WriteInt16(0); // error_code

        if (header.ApiVersion >= 3)
        {
            // members array response
            if (isFlexible)
            {
                writer.WriteCompactArrayLength(0);
            }
            else
            {
                writer.WriteArrayLength(0);
            }
        }

        if (isFlexible)
            writer.WriteEmptyTagBuffer();
    }
}
