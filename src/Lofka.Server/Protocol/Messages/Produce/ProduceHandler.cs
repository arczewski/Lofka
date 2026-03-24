using Lofka.Server.Configuration;
using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Produce;

public static class ProduceHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body, BigEndianWriter writer,
        TopicStore topicStore, ServerConfig config)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 9;

        // transactional_id (v3+)
        if (header.ApiVersion >= 3)
        {
            if (isFlexible)
                reader.ReadCompactNullableString();
            else
                reader.ReadNullableString();
        }

        short acks = reader.ReadInt16();
        int timeoutMs = reader.ReadInt32();

        // topics array
        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var results = new List<(string Topic, List<(int Partition, short ErrorCode, long BaseOffset)> Partitions)>();

        for (int t = 0; t < topicCount; t++)
        {
            string topicName = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            var topic = topicStore.GetOrCreateTopic(topicName);

            int partCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            var partResults = new List<(int Partition, short ErrorCode, long BaseOffset)>();

            for (int p = 0; p < partCount; p++)
            {
                int partitionIndex = reader.ReadInt32();

                // record_set is RECORDS type — read as non-compact bytes (int32 length)
                int recordSetSize = reader.ReadInt32();

                if (recordSetSize <= 0 || partitionIndex < 0 || partitionIndex >= topic.Partitions.Length)
                {
                    if (recordSetSize > 0) reader.Skip(recordSetSize);
                    partResults.Add((partitionIndex, 3, -1)); // UNKNOWN_TOPIC_OR_PARTITION
                    continue;
                }

                var recordSetBytes = reader.ReadRawBytes(recordSetSize).ToArray();
                long baseOffset = topic.Partitions[partitionIndex].Append(recordSetBytes);
                partResults.Add((partitionIndex, 0, baseOffset));

                if (isFlexible) reader.SkipTagBuffer();
            }

            results.Add((topicName, partResults));
            if (isFlexible) reader.SkipTagBuffer();
        }

        if (isFlexible) reader.SkipTagBuffer();

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // responses array
        if (isFlexible)
        {
            writer.WriteCompactArrayLength(results.Count);
            foreach (var (topicName, partitions) in results)
            {
                writer.WriteCompactString(topicName);
                writer.WriteCompactArrayLength(partitions.Count);
                foreach (var (partIdx, errorCode, baseOffset) in partitions)
                {
                    writer.WriteInt32(partIdx);
                    writer.WriteInt16(errorCode);
                    writer.WriteInt64(baseOffset);
                    // log_append_time (v2+)
                    if (header.ApiVersion >= 2)
                        writer.WriteInt64(-1);
                    // log_start_offset (v5+)
                    if (header.ApiVersion >= 5)
                        writer.WriteInt64(0);
                    if (isFlexible)
                    {
                        // record_errors (v8+)
                        writer.WriteCompactArrayLength(0);
                        // error_message
                        writer.WriteCompactNullableString(null);
                        writer.WriteEmptyTagBuffer();
                    }
                }
                writer.WriteEmptyTagBuffer();
            }
        }
        else
        {
            writer.WriteArrayLength(results.Count);
            foreach (var (topicName, partitions) in results)
            {
                writer.WriteString(topicName);
                writer.WriteArrayLength(partitions.Count);
                foreach (var (partIdx, errorCode, baseOffset) in partitions)
                {
                    writer.WriteInt32(partIdx);
                    writer.WriteInt16(errorCode);
                    writer.WriteInt64(baseOffset);
                    if (header.ApiVersion >= 2)
                        writer.WriteInt64(-1); // log_append_time
                    if (header.ApiVersion >= 5)
                        writer.WriteInt64(0);  // log_start_offset
                    if (header.ApiVersion >= 8)
                    {
                        writer.WriteArrayLength(0); // record_errors
                        writer.WriteNullableString(null); // error_message
                    }
                }
            }
        }

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        if (isFlexible)
            writer.WriteEmptyTagBuffer();
    }
}
