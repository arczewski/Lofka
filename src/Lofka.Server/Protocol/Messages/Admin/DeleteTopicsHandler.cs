using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;
using Lofka.Server.Storage;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class DeleteTopicsHandler
{
    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body,
        BigEndianWriter writer, TopicStore topicStore)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 4;

        int topicCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var results = new List<(string Name, short ErrorCode)>();

        for (int i = 0; i < topicCount; i++)
        {
            string name = isFlexible ? reader.ReadCompactString() : reader.ReadString();
            bool deleted = topicStore.DeleteTopic(name);
            results.Add((name, deleted ? (short)0 : (short)3)); // UNKNOWN_TOPIC_OR_PARTITION
        }

        // timeout_ms
        reader.ReadInt32();

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);

        // throttle_time_ms (v1+)
        if (header.ApiVersion >= 1)
            writer.WriteInt32(0);

        if (isFlexible)
        {
            writer.WriteCompactArrayLength(results.Count);
            foreach (var (name, errorCode) in results)
            {
                writer.WriteCompactString(name);
                writer.WriteInt16(errorCode);
                writer.WriteCompactNullableString(null); // error_message
                writer.WriteEmptyTagBuffer();
            }
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(results.Count);
            foreach (var (name, errorCode) in results)
            {
                writer.WriteString(name);
                writer.WriteInt16(errorCode);
            }
        }
    }
}
