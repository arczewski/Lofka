using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.ListOffsets;

public static class ListOffsetsHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body, BigEndianWriter writer,
        TopicStore topicStore)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 6;

        int replicaId = reader.ReadInt32();

        byte isolationLevel = 0;
        if (header.ApiVersion >= 2)
            isolationLevel = reader.ReadInt8();

        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var results = new List<(string Topic, List<(int Partition, short ErrorCode, long Timestamp, long Offset)> Parts)>();

        for (int t = 0; t < topicCount; t++)
        {
            string topicName = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            int partCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

            var topic = topicStore.GetTopic(topicName);
            var partResults = new List<(int Partition, short ErrorCode, long Timestamp, long Offset)>();

            for (int p = 0; p < partCount; p++)
            {
                int partition = reader.ReadInt32();
                if (header.ApiVersion >= 4)
                    reader.ReadInt32(); // current_leader_epoch
                long timestamp = reader.ReadInt64();
                if (isFlexible) reader.SkipTagBuffer();

                if (topic == null || partition < 0 || partition >= topic.Partitions.Length)
                {
                    partResults.Add((partition, 3, -1, -1)); // UNKNOWN_TOPIC_OR_PARTITION
                    continue;
                }

                var log = topic.Partitions[partition];
                long offset;

                if (timestamp == -2) // EARLIEST
                    offset = 0;
                else if (timestamp == -1) // LATEST
                    offset = log.NextOffset;
                else
                    offset = 0; // For other timestamps, return beginning

                partResults.Add((partition, 0, timestamp, offset));
            }

            results.Add((topicName, partResults));
            if (isFlexible) reader.SkipTagBuffer();
        }

        if (isFlexible) reader.SkipTagBuffer();

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v2+)
        if (header.ApiVersion >= 2)
            writer.WriteInt32(0);

        if (isFlexible)
        {
            writer.WriteCompactArrayLength(results.Count);
            foreach (var (topicName, partitions) in results)
            {
                writer.WriteCompactString(topicName);
                writer.WriteCompactArrayLength(partitions.Count);
                foreach (var (partIdx, errorCode, timestamp, offset) in partitions)
                {
                    writer.WriteInt32(partIdx);
                    writer.WriteInt16(errorCode);
                    writer.WriteInt64(timestamp);
                    writer.WriteInt64(offset);
                    if (header.ApiVersion >= 4)
                        writer.WriteInt32(0); // leader_epoch
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
                foreach (var (partIdx, errorCode, timestamp, offset) in partitions)
                {
                    writer.WriteInt32(partIdx);
                    writer.WriteInt16(errorCode);
                    writer.WriteInt64(timestamp);
                    writer.WriteInt64(offset);
                    if (header.ApiVersion >= 4)
                        writer.WriteInt32(0); // leader_epoch
                }
            }
        }
    }
}
