using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class CreateTopicsHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, TopicStore topicStore)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 5;

        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var results = new List<(string Name, short ErrorCode, string? ErrorMessage)>();

        for (int i = 0; i < topicCount; i++)
        {
            string name = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            int numPartitions = reader.ReadInt32();
            short replicationFactor = reader.ReadInt16();

            // assignments array
            int assignmentCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            for (int a = 0; a < assignmentCount; a++)
            {
                reader.ReadInt32(); // partition_index
                int replicaCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
                for (int r = 0; r < replicaCount; r++)
                    reader.ReadInt32();
                if (isFlexible) reader.SkipTagBuffer();
            }

            // configs array
            int configCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            for (int c = 0; c < configCount; c++)
            {
                if (isFlexible) reader.ReadCompactString(); // name
                else reader.ReadString();
                if (isFlexible) reader.ReadCompactNullableString(); // value
                else reader.ReadNullableString();
                if (isFlexible) reader.SkipTagBuffer();
            }

            if (isFlexible) reader.SkipTagBuffer();

            if (numPartitions <= 0) numPartitions = 1;
            var (_, errorCode) = topicStore.CreateTopic(name, numPartitions, failIfExists: true);
            string? errorMessage = errorCode == 36 ? "Topic already exists" : null;
            results.Add((name, errorCode, errorMessage));
        }

        // timeout_ms
        reader.ReadInt32();
        // validate_only (v1+)
        if (header.ApiVersion >= 1)
            reader.ReadBool();

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v2+)
        if (header.ApiVersion >= 2)
            writer.WriteInt32(0);

        if (isFlexible)
        {
            writer.WriteCompactArrayLength(results.Count);
            foreach (var (name, errorCode, errorMessage) in results)
            {
                writer.WriteCompactString(name);
                // topic_id (v7+) — not in our version range
                writer.WriteInt16(errorCode);
                writer.WriteCompactNullableString(errorMessage);

                // num_partitions (v5+)
                if (header.ApiVersion >= 5)
                {
                    var topic = topicStore.GetTopic(name);
                    writer.WriteInt32(topic?.Partitions.Length ?? 0);
                    writer.WriteInt16(1); // replication_factor
                    writer.WriteCompactArrayLength(0); // configs
                }

                writer.WriteEmptyTagBuffer();
            }
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(results.Count);
            foreach (var (name, errorCode, errorMessage) in results)
            {
                writer.WriteString(name);
                writer.WriteInt16(errorCode);
                writer.WriteNullableString(errorMessage);

                if (header.ApiVersion >= 5)
                {
                    var topic = topicStore.GetTopic(name);
                    writer.WriteInt32(topic?.Partitions.Length ?? 0);
                    writer.WriteInt16(1);
                    writer.WriteArrayLength(0);
                }
            }
        }
    }
}
