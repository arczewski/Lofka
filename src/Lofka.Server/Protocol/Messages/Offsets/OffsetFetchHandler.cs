using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Offsets;

public static class OffsetFetchHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, OffsetStore offsetStore)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 6;

        string groupId = isFlexible ? reader.ReadCompactString() : reader.ReadString();

        // topics array (nullable in some versions)
        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var results = new List<(string Topic, List<(int Partition, long Offset, string? Metadata)> Parts)>();

        if (topicCount >= 0)
        {
            for (int t = 0; t < topicCount; t++)
            {
                string topicName = isFlexible ? reader.ReadCompactString() : reader.ReadString();
                int partCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
                var partResults = new List<(int, long, string?)>();

                for (int p = 0; p < partCount; p++)
                {
                    int partition = reader.ReadInt32();
                    // partition_indexes is a flat array of INT32, no per-element tag buffer
                    long? offset = offsetStore.GetOffset(groupId, topicName, partition);
                    partResults.Add((partition, offset ?? -1, null));
                }

                results.Add((topicName, partResults));
                if (isFlexible) reader.SkipTagBuffer(); // per-topic tag buffer
            }
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
                foreach (var (partition, offset, metadata) in partitions)
                {
                    writer.WriteInt32(partition);
                    writer.WriteInt64(offset);
                    if (header.ApiVersion >= 5)
                        writer.WriteInt32(-1); // leader_epoch
                    writer.WriteCompactNullableString(metadata);
                    writer.WriteInt16(0); // error_code
                    writer.WriteEmptyTagBuffer();
                }
                writer.WriteEmptyTagBuffer();
            }

            // error_code (v2+)
            if (header.ApiVersion >= 2)
                writer.WriteInt16(0);

            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(results.Count);
            foreach (var (topicName, partitions) in results)
            {
                writer.WriteString(topicName);
                writer.WriteArrayLength(partitions.Count);
                foreach (var (partition, offset, metadata) in partitions)
                {
                    writer.WriteInt32(partition);
                    writer.WriteInt64(offset);
                    if (header.ApiVersion >= 5)
                        writer.WriteInt32(-1); // leader_epoch
                    writer.WriteNullableString(metadata);
                    writer.WriteInt16(0); // error_code
                }
            }

            if (header.ApiVersion >= 2)
                writer.WriteInt16(0); // error_code
        }
    }
}
