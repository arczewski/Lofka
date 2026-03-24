using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Offsets;

public static class OffsetCommitHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, OffsetStore offsetStore)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 8;

        string groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();
        int generationId = reader.ReadInt32();
        string memberId = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        // group_instance_id (v7+)
        if (header.ApiVersion >= 7)
        {
            if (isFlexible) reader.ReadCompactNullableString();
            else reader.ReadNullableString();
        }

        // retention_time_ms (v2-v4)
        if (header.ApiVersion >= 2 && header.ApiVersion <= 4)
            reader.ReadInt64();

        // topics array
        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var results = new List<(string Topic, List<(int Partition, short ErrorCode)> Parts)>();

        for (int t = 0; t < topicCount; t++)
        {
            string topicName = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            int partCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            var partResults = new List<(int, short)>();

            for (int p = 0; p < partCount; p++)
            {
                int partition = reader.ReadInt32();
                long offset = reader.ReadInt64();

                // leader_epoch (v6+)
                if (header.ApiVersion >= 6)
                    reader.ReadInt32();

                // committed_metadata
                if (isFlexible) reader.ReadCompactNullableString();
                else reader.ReadNullableString();

                // commit_timestamp (v1 only)
                // Not applicable since we start at v2

                if (isFlexible) reader.SkipTagBuffer();

                offsetStore.Commit(groupId, topicName, partition, offset);
                partResults.Add((partition, 0));
            }

            results.Add((topicName, partResults));
            if (isFlexible) reader.SkipTagBuffer();
        }

        if (isFlexible) reader.SkipTagBuffer();

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v3+)
        if (header.ApiVersion >= 3)
            writer.WriteInt32(0);

        if (isFlexible)
        {
            writer.WriteCompactArrayLength(results.Count);
            foreach (var (topicName, partitions) in results)
            {
                writer.WriteCompactString(topicName);
                writer.WriteCompactArrayLength(partitions.Count);
                foreach (var (partition, errorCode) in partitions)
                {
                    writer.WriteInt32(partition);
                    writer.WriteInt16(errorCode);
                    writer.WriteEmptyTagBuffer();
                }
                writer.WriteEmptyTagBuffer();
            }
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(results.Count);
            foreach (var (topicName, partitions) in results)
            {
                writer.WriteString(topicName);
                writer.WriteArrayLength(partitions.Count);
                foreach (var (partition, errorCode) in partitions)
                {
                    writer.WriteInt32(partition);
                    writer.WriteInt16(errorCode);
                }
            }
        }
    }
}
