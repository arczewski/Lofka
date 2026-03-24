using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Fetch;

public static class FetchHandler
{
    public static async Task HandleAsync(RequestHeader header, ReadOnlyMemory<byte> body,
        BigEndianWriter writer, TopicStore topicStore, CancellationToken ct)
    {
        var reader = new BigEndianReader(body.Span);
        bool isFlexible = header.ApiVersion >= 12;

        // Parse request
        int replicaId = reader.ReadInt32();
        int maxWaitMs = reader.ReadInt32();
        int minBytes = reader.ReadInt32();

        int maxBytes = 0;
        if (header.ApiVersion >= 3)
            maxBytes = reader.ReadInt32();

        byte isolationLevel = 0;
        if (header.ApiVersion >= 4)
            isolationLevel = reader.ReadInt8();

        int sessionId = 0;
        int sessionEpoch = -1;
        if (header.ApiVersion >= 7)
        {
            sessionId = reader.ReadInt32();
            sessionEpoch = reader.ReadInt32();
        }

        // topics array
        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var fetchRequests = new List<(string Topic, List<(int Partition, long FetchOffset, int MaxBytes)> Parts)>();

        for (int t = 0; t < topicCount; t++)
        {
            string topicName = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            int partCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            var parts = new List<(int, long, int)>();

            for (int p = 0; p < partCount; p++)
            {
                int partition = reader.ReadInt32();
                if (header.ApiVersion >= 9)
                    reader.ReadInt32(); // current_leader_epoch
                long fetchOffset = reader.ReadInt64();
                if (header.ApiVersion >= 5)
                    reader.ReadInt64(); // log_start_offset
                int partMaxBytes = reader.ReadInt32();
                parts.Add((partition, fetchOffset, partMaxBytes));
                if (isFlexible) reader.SkipTagBuffer();
            }

            fetchRequests.Add((topicName, parts));
            if (isFlexible) reader.SkipTagBuffer();
        }
        // Skip forgotten topics, rack_id, etc.

        // Check if we have data; if not, long-poll
        bool hasData = false;
        var waitTasks = new List<Task>();

        foreach (var (topicName, parts) in fetchRequests)
        {
            var topic = topicStore.GetTopic(topicName);
            if (topic == null) continue;

            foreach (var (partition, fetchOffset, _) in parts)
            {
                if (partition < 0 || partition >= topic.Partitions.Length) continue;
                var log = topic.Partitions[partition];
                if (log.NextOffset > fetchOffset)
                {
                    hasData = true;
                    break;
                }
                waitTasks.Add(log.WaitForDataAsync(maxWaitMs, ct));
            }
            if (hasData) break;
        }

        if (!hasData && waitTasks.Count > 0)
        {
            await Task.WhenAny(waitTasks);
        }

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        // error_code (v7+)
        if (header.ApiVersion >= 7)
            writer.WriteInt16(0);

        // session_id (v7+)
        if (header.ApiVersion >= 7)
            writer.WriteInt32(0);

        // responses array
        if (isFlexible)
        {
            writer.WriteCompactArrayLength(fetchRequests.Count);
        }
        else
        {
            writer.WriteArrayLength(fetchRequests.Count);
        }

        foreach (var (topicName, parts) in fetchRequests)
        {
            if (isFlexible) writer.WriteCompactString(topicName);
            else writer.WriteString(topicName);

            var topic = topicStore.GetTopic(topicName);

            if (isFlexible) writer.WriteCompactArrayLength(parts.Count);
            else writer.WriteArrayLength(parts.Count);

            foreach (var (partition, fetchOffset, partMaxBytes) in parts)
            {
                writer.WriteInt32(partition); // partition_index

                if (topic == null || partition < 0 || partition >= topic.Partitions.Length)
                {
                    writer.WriteInt16(3); // UNKNOWN_TOPIC_OR_PARTITION
                    writer.WriteInt64(0); // high_watermark
                    if (header.ApiVersion >= 4)
                        writer.WriteInt64(0); // last_stable_offset
                    if (header.ApiVersion >= 5)
                        writer.WriteInt64(0); // log_start_offset
                    if (header.ApiVersion >= 4)
                    {
                        // aborted_transactions
                        if (isFlexible) writer.WriteCompactArrayLength(0);
                        else writer.WriteArrayLength(-1);
                    }
                    if (header.ApiVersion >= 11)
                        writer.WriteInt32(-1); // preferred_read_replica
                    // records (null)
                    if (isFlexible) writer.WriteCompactNullableBytes(null);
                    else writer.WriteNullableBytes(null);
                    if (isFlexible) writer.WriteEmptyTagBuffer();
                    continue;
                }

                var log = topic.Partitions[partition];
                var (batches, highWatermark) = log.Read(fetchOffset, partMaxBytes);

                writer.WriteInt16(0); // error_code
                writer.WriteInt64(highWatermark); // high_watermark
                if (header.ApiVersion >= 4)
                    writer.WriteInt64(highWatermark); // last_stable_offset
                if (header.ApiVersion >= 5)
                    writer.WriteInt64(0); // log_start_offset
                if (header.ApiVersion >= 4)
                {
                    // aborted_transactions (empty)
                    if (isFlexible) writer.WriteCompactArrayLength(0);
                    else writer.WriteArrayLength(-1);
                }
                if (header.ApiVersion >= 11)
                    writer.WriteInt32(-1); // preferred_read_replica

                // records - concatenated raw record batches
                int totalSize = 0;
                foreach (var batch in batches) totalSize += batch.Length;

                if (totalSize == 0)
                {
                    // Empty records = 0-length byte array (not null/-1 which librdkafka rejects)
                    if (isFlexible) writer.WriteUnsignedVarint(1); // compact: length+1 = 0+1 = 1
                    else writer.WriteInt32(0);
                }
                else
                {
                    // Write as bytes (int32 length prefix for non-flexible, compact for flexible)
                    if (isFlexible)
                    {
                        writer.WriteUnsignedVarint((uint)(totalSize + 1));
                    }
                    else
                    {
                        writer.WriteInt32(totalSize);
                    }
                    foreach (var batch in batches)
                        writer.WriteRawBytes(batch);
                }

                if (isFlexible) writer.WriteEmptyTagBuffer();
            }
            if (isFlexible) writer.WriteEmptyTagBuffer();
        }

        if (isFlexible)
            writer.WriteEmptyTagBuffer();
    }
}
