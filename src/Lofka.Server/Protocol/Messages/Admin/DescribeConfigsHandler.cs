using Lofka.Server.Protocol.Headers;
using Lofka.Server.Protocol.Primitives;

namespace Lofka.Server.Protocol.Messages.Admin;

public static class DescribeConfigsHandler
{
    // ResourceType: 2 = Topic, 4 = Broker
    // ConfigSource: 5 = DEFAULT_CONFIG
    private static readonly (string Name, string Value)[] TopicConfigs =
    [
        ("cleanup.policy", "delete"),
        ("compression.type", "producer"),
        ("delete.retention.ms", "86400000"),
        ("file.delete.delay.ms", "60000"),
        ("flush.messages", "9223372036854775807"),
        ("flush.ms", "9223372036854775807"),
        ("index.interval.bytes", "4096"),
        ("max.compaction.lag.ms", "9223372036854775807"),
        ("max.message.bytes", "1048588"),
        ("message.timestamp.type", "CreateTime"),
        ("min.cleanable.dirty.ratio", "0.5"),
        ("min.compaction.lag.ms", "0"),
        ("min.insync.replicas", "1"),
        ("retention.bytes", "-1"),
        ("retention.ms", "604800000"),
        ("segment.bytes", "1073741824"),
        ("segment.index.bytes", "10485760"),
        ("segment.jitter.ms", "0"),
        ("segment.ms", "604800000"),
    ];

    private static readonly (string Name, string Value)[] BrokerConfigs =
    [
        ("log.retention.ms", "604800000"),
        ("log.segment.bytes", "1073741824"),
        ("log.retention.bytes", "-1"),
        ("min.insync.replicas", "1"),
        ("num.partitions", "1"),
        ("default.replication.factor", "1"),
        ("log.cleanup.policy", "delete"),
        ("log.segment.delete.delay.ms", "60000"),
        ("message.max.bytes", "1048588"),
    ];

    public static void Handle(RequestHeader header, ReadOnlySpan<byte> body, BigEndianWriter writer)
    {
        var reader = new BigEndianReader(body);
        bool isFlexible = header.ApiVersion >= 4;

        int resourceCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();

        var resources = new List<(byte ResourceType, string ResourceName, List<string>? RequestedNames)>();

        for (int i = 0; i < resourceCount; i++)
        {
            byte resourceType = reader.ReadInt8();
            string resourceName = isFlexible ? reader.ReadCompactString() : reader.ReadString();

            int configNameCount = isFlexible ? reader.ReadCompactArrayLength() : reader.ReadArrayLength();
            List<string>? requestedNames = null;
            if (configNameCount > 0)
            {
                requestedNames = new List<string>(configNameCount);
                for (int c = 0; c < configNameCount; c++)
                {
                    requestedNames.Add(isFlexible ? reader.ReadCompactString() : reader.ReadString());
                    if (isFlexible) reader.SkipTagBuffer();
                }
            }

            if (isFlexible) reader.SkipTagBuffer();
            resources.Add((resourceType, resourceName, requestedNames));
        }

        // Write response
        ResponseHeader.Write(writer, header.CorrelationId, header.ApiKey, header.ApiVersion);
        writer.WriteInt32(0); // throttle_time_ms

        if (isFlexible)
        {
            writer.WriteCompactArrayLength(resources.Count);
            foreach (var (resourceType, resourceName, requestedNames) in resources)
            {
                writer.WriteInt16(0); // error_code
                writer.WriteCompactNullableString(null); // error_message
                writer.WriteInt8(resourceType);
                writer.WriteCompactString(resourceName);
                WriteConfigs(writer, resourceType, requestedNames, isFlexible: true, header.ApiVersion);
                writer.WriteEmptyTagBuffer();
            }
            writer.WriteEmptyTagBuffer();
        }
        else
        {
            writer.WriteArrayLength(resources.Count);
            foreach (var (resourceType, resourceName, requestedNames) in resources)
            {
                writer.WriteInt16(0);
                writer.WriteNullableString(null);
                writer.WriteInt8(resourceType);
                writer.WriteString(resourceName);
                WriteConfigs(writer, resourceType, requestedNames, isFlexible: false, header.ApiVersion);
            }
        }
    }

    private static void WriteConfigs(BigEndianWriter writer, byte resourceType,
        List<string>? requestedNames, bool isFlexible, short apiVersion)
    {
        var allConfigs = resourceType == 2 ? TopicConfigs : BrokerConfigs;

        // Filter to requested names if specified
        var configs = requestedNames != null
            ? allConfigs.Where(c => requestedNames.Contains(c.Name)).ToArray()
            : allConfigs;

        if (isFlexible)
        {
            writer.WriteCompactArrayLength(configs.Length);
            foreach (var (name, value) in configs)
            {
                writer.WriteCompactString(name);
                writer.WriteCompactNullableString(value);
                writer.WriteBool(false); // read_only
                // config_source (v1+): 5 = DEFAULT_CONFIG
                if (apiVersion >= 1)
                    writer.WriteInt8(5);
                writer.WriteBool(false); // is_sensitive
                // synonyms (v1+)
                if (apiVersion >= 1)
                    writer.WriteCompactArrayLength(0);
                // config_type (v3+)
                if (apiVersion >= 3)
                    writer.WriteInt8(1); // STRING
                // documentation (v3+)
                if (apiVersion >= 3)
                    writer.WriteCompactNullableString(null);
                writer.WriteEmptyTagBuffer();
            }
        }
        else
        {
            writer.WriteArrayLength(configs.Length);
            foreach (var (name, value) in configs)
            {
                writer.WriteString(name);
                writer.WriteNullableString(value);
                writer.WriteBool(false); // read_only
                if (apiVersion >= 1)
                    writer.WriteInt8(5); // config_source
                writer.WriteBool(false); // is_sensitive
                if (apiVersion >= 1)
                {
                    writer.WriteArrayLength(0); // synonyms
                }
                if (apiVersion >= 3)
                {
                    writer.WriteInt8(1); // config_type
                    writer.WriteNullableString(null); // documentation
                }
            }
        }
    }
}
