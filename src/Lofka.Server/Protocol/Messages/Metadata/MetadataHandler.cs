using Lofka.Server.Configuration;
using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Metadata;

public static class MetadataHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body, BigEndianWriter writer,
        TopicStore topicStore, ServerConfig config)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 9;

        // Parse requested topics
        List<string>? requestedTopics = null;
        bool allowAutoCreate = config.AutoCreateTopics;

        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        if (topicCount >= 0)
        {
            requestedTopics = new List<string>(topicCount);
            for (int i = 0; i < topicCount; i++)
            {
                if (header.ApiVersion >= 10)
                {
                    // v10+: topic_id (UUID) - skip it
                    reader.Skip(16);
                }
                string? name = isFlexible ? reader.ReadCompactNullableString() : reader.ReadNullableString();
                if (name != null) requestedTopics.Add(name);
                if (isFlexible) reader.SkipTagBuffer();
            }
        }

        if (header.ApiVersion >= 4)
        {
            allowAutoCreate = reader.ReadBool();
        }
        // skip remaining fields (allow_auto_topic_creation, include_cluster_authorized_operations, include_topic_authorized_operations)

        // Determine topics to return
        IEnumerable<TopicInfo> topics;
        if (requestedTopics == null)
        {
            // null means all topics
            topics = topicStore.GetAllTopics();
        }
        else
        {
            var result = new List<TopicInfo>();
            foreach (var name in requestedTopics)
            {
                var topic = topicStore.GetTopic(name);
                if (topic == null && allowAutoCreate)
                {
                    topic = topicStore.GetOrCreateTopic(name);
                }
                if (topic != null) result.Add(topic);
            }
            topics = result;
        }

        var topicList = topics.ToList();

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v3+)
        if (header.ApiVersion >= 3)
            writer.WriteInt32(0);

        // Brokers array
        if (isFlexible)
        {
            writer.WriteCompactArrayLength(1);
            writer.WriteInt32(0); // node_id
            writer.WriteCompactString(config.AdvertisedHost);
            writer.WriteInt32(config.Port);
            writer.WriteCompactNullableString(null); // rack
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(1);
            writer.WriteInt32(0); // node_id
            writer.WriteString(config.AdvertisedHost);
            writer.WriteInt32(config.Port);
            if (header.ApiVersion >= 1)
                writer.WriteNullableString(null); // rack
        }

        // cluster_id (v2+)
        if (header.ApiVersion >= 2)
        {
            if (isFlexible)
                writer.WriteCompactNullableString("lofka-cluster");
            else
                writer.WriteNullableString("lofka-cluster");
        }

        // controller_id (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        // Topics array
        if (isFlexible)
        {
            writer.WriteCompactArrayLength(topicList.Count);
            foreach (var topic in topicList)
            {
                writer.WriteInt16(0); // error_code
                writer.WriteCompactString(topic.Name);

                if (header.ApiVersion >= 10)
                {
                    // topic_id (UUID as 16 bytes)
                    WriteUuid(writer, topic.TopicId);
                }

                // is_internal
                if (header.ApiVersion >= 1)
                    writer.WriteBool(false);

                // Partitions
                writer.WriteCompactArrayLength(topic.Partitions.Length);
                for (int p = 0; p < topic.Partitions.Length; p++)
                {
                    writer.WriteInt16(0); // error_code
                    writer.WriteInt32(p); // partition_index
                    writer.WriteInt32(0); // leader_id
                    if (header.ApiVersion >= 7)
                        writer.WriteInt32(0); // leader_epoch
                    writer.WriteCompactArrayLength(1); // replica_nodes
                    writer.WriteInt32(0);
                    writer.WriteCompactArrayLength(1); // isr_nodes
                    writer.WriteInt32(0);
                    if (header.ApiVersion >= 5)
                    {
                        writer.WriteCompactArrayLength(0); // offline_replicas
                    }
                    writer.WriteEmptyTagBuffer();
                }

                // topic_authorized_operations (v8+)
                if (header.ApiVersion >= 8)
                    writer.WriteInt32(-2147483648); // INT32_MIN = unknown

                writer.WriteEmptyTagBuffer();
            }
        }
        else
        {
            writer.WriteArrayLength(topicList.Count);
            foreach (var topic in topicList)
            {
                writer.WriteInt16(0); // error_code
                writer.WriteString(topic.Name);
                if (header.ApiVersion >= 1)
                    writer.WriteBool(false); // is_internal

                writer.WriteArrayLength(topic.Partitions.Length);
                for (int p = 0; p < topic.Partitions.Length; p++)
                {
                    writer.WriteInt16(0); // error_code
                    writer.WriteInt32(p); // partition_index
                    writer.WriteInt32(0); // leader_id
                    if (header.ApiVersion >= 7)
                        writer.WriteInt32(0); // leader_epoch
                    writer.WriteArrayLength(1); // replica_nodes
                    writer.WriteInt32(0);
                    writer.WriteArrayLength(1); // isr_nodes
                    writer.WriteInt32(0);
                    if (header.ApiVersion >= 5)
                    {
                        writer.WriteArrayLength(0); // offline_replicas
                    }
                }

                if (header.ApiVersion >= 8)
                    writer.WriteInt32(-2147483648);
            }
        }

        // cluster_authorized_operations (v8+)
        if (header.ApiVersion >= 8)
            writer.WriteInt32(-2147483648);

        if (isFlexible)
            writer.WriteEmptyTagBuffer();
    }

    private static void WriteUuid(BigEndianWriter writer, Guid guid)
    {
        Span<byte> bytes = stackalloc byte[16];
        guid.TryWriteBytes(bytes);
        // Kafka uses big-endian UUID (most significant bits first)
        // .NET Guid has mixed endianness for first 3 components
        writer.WriteRawBytes(bytes);
    }
}
