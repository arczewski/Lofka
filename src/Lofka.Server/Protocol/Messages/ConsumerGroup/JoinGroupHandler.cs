using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.ConsumerGroup;

public static class JoinGroupHandler
{
    public static async Task HandleAsync(RequestHeader header, ReadOnlyMemory<byte> body,
        BigEndianWriter writer, ConsumerGroupManager groupManager, CancellationToken ct)
    {
        var reader = new BigEndianReader(body.Span);
        bool isFlexible = header.ApiVersion >= 6;

        string groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        int sessionTimeoutMs = reader.ReadInt32();

        int rebalanceTimeoutMs = sessionTimeoutMs;
        if (header.ApiVersion >= 1)
            rebalanceTimeoutMs = reader.ReadInt32();

        string memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        string? groupInstanceId = null;
        if (header.ApiVersion >= 5)
            groupInstanceId = isFlexible ? reader.ReadCompactNullableString() : reader.ReadNullableString();

        string protocolType = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        // protocols array
        int protocolCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
        var protocols = new List<string>();
        byte[] protocolMetadata = Array.Empty<byte>();

        for (int i = 0; i < protocolCount; i++)
        {
            string name = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            byte[] metadata = isFlexible ? reader.ReadCompactBytes() : reader.ReadBytes();
            protocols.Add(name);
            if (i == 0) protocolMetadata = metadata; // Use first protocol's metadata
            if (isFlexible) reader.SkipTagBuffer();
        }

        if (isFlexible) reader.SkipTagBuffer();

        // Join the group
        var group = groupManager.GetOrCreateGroup(groupId);
        var result = await group.JoinAsync(memberId, groupInstanceId, header.ClientId ?? "",
            "localhost", protocols, protocolMetadata, sessionTimeoutMs, ct);

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v2+)
        if (header.ApiVersion >= 2)
            writer.WriteInt32(0);

        writer.WriteInt16(result.ErrorCode);
        writer.WriteInt32(result.GenerationId);

        if (isFlexible)
        {
            writer.WriteCompactString(result.ProtocolName);
            writer.WriteCompactString(result.LeaderId);
            writer.WriteCompactString(result.MemberId);

            // members array
            writer.WriteCompactArrayLength(result.Members.Count);
            foreach (var (mid, metadata) in result.Members)
            {
                writer.WriteCompactString(mid);
                if (header.ApiVersion >= 5)
                    writer.WriteCompactNullableString(null); // group_instance_id
                writer.WriteCompactNullableBytes(metadata);
                writer.WriteEmptyTagBuffer();
            }
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteString(result.ProtocolName);
            writer.WriteString(result.LeaderId);
            writer.WriteString(result.MemberId);

            writer.WriteArrayLength(result.Members.Count);
            foreach (var (mid, metadata) in result.Members)
            {
                writer.WriteString(mid);
                if (header.ApiVersion >= 5)
                    writer.WriteNullableString(null); // group_instance_id
                writer.WriteNullableBytes(metadata);
            }
        }
    }
}
