using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.ConsumerGroup;

public static class SyncGroupHandler
{
    public static async Task HandleAsync(RequestHeader header, ReadOnlyMemory<byte> body,
        BigEndianWriter writer, ConsumerGroupManager groupManager, CancellationToken ct)
    {
        var reader = new BigEndianReader(body.Span);
        bool isFlexible = header.ApiVersion >= 4;

        string groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        int generationId = reader.ReadInt32();
        string memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        string? groupInstanceId = null;
        if (header.ApiVersion >= 3)
            groupInstanceId = isFlexible ? reader.ReadCompactNullableString() : reader.ReadNullableString();

        // protocol_type (v5+)
        if (header.ApiVersion >= 5)
        {
            if (isFlexible) reader.ReadCompactNullableString();
            else reader.ReadNullableString();
        }
        // protocol_name (v5+)
        if (header.ApiVersion >= 5)
        {
            if (isFlexible) reader.ReadCompactNullableString();
            else reader.ReadNullableString();
        }

        // assignments array
        int assignmentCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
        List<(string MemberId, byte[] Assignment)>? assignments = null;

        if (assignmentCount > 0)
        {
            assignments = new List<(string, byte[])>();
            for (int i = 0; i < assignmentCount; i++)
            {
                string mid = isFlexible ? reader.ReadCompactString() : reader.ReadString();
                byte[] assignment = isFlexible ? reader.ReadCompactBytes() : reader.ReadBytes();
                assignments.Add((mid, assignment));
                if (isFlexible) reader.SkipTagBuffer();
            }
        }

        if (isFlexible) reader.SkipTagBuffer();

        var group = groupManager.GetOrCreateGroup(groupId);
        var (errorCode, assignmentData) = await group.SyncAsync(memberId, generationId, assignments, ct);

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        writer.WriteInt16(errorCode);

        // protocol_type (v5+)
        if (header.ApiVersion >= 5)
        {
            if (isFlexible) writer.WriteCompactNullableString(null);
            else writer.WriteNullableString(null);
        }
        // protocol_name (v5+)
        if (header.ApiVersion >= 5)
        {
            if (isFlexible) writer.WriteCompactNullableString(null);
            else writer.WriteNullableString(null);
        }

        // assignment
        if (isFlexible)
        {
            writer.WriteCompactNullableBytes(assignmentData);
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteNullableBytes(assignmentData);
        }
    }
}
