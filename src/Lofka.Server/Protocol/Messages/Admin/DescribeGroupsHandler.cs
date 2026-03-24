using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class DescribeGroupsHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, ConsumerGroupManager groupManager)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 5;

        int groupCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
        var groupIds = new List<string>();
        for (int i = 0; i < groupCount; i++)
        {
            groupIds.Add(isFlexible ? reader.ReadCompactString() : reader.ReadString());
        }

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        if (isFlexible)
        {
            writer.WriteCompactArrayLength(groupIds.Count);
            foreach (var groupId in groupIds)
            {
                var group = groupManager.GetGroup(groupId);
                writer.WriteInt16(0); // error_code
                writer.WriteCompactString(groupId);
                writer.WriteCompactString(group != null ? group.Status.ToString() : "Dead"); // state
                writer.WriteCompactString("consumer"); // protocol_type
                writer.WriteCompactString(group?.ProtocolName ?? ""); // protocol_data
                writer.WriteCompactArrayLength(0); // members (empty for simplicity)
                if (header.ApiVersion >= 3)
                    writer.WriteInt32(-2147483648); // authorized_operations
                writer.WriteEmptyTagBuffer();
            }
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(groupIds.Count);
            foreach (var groupId in groupIds)
            {
                var group = groupManager.GetGroup(groupId);
                writer.WriteInt16(0);
                writer.WriteString(groupId);
                writer.WriteString(group != null ? group.Status.ToString() : "Dead");
                writer.WriteString("consumer");
                writer.WriteString(group?.ProtocolName ?? "");
                writer.WriteArrayLength(0); // members
                if (header.ApiVersion >= 3)
                    writer.WriteInt32(-2147483648);
            }
        }
    }
}
