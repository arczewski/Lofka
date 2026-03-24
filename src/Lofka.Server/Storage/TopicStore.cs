using System.Collections.Concurrent;
using Lofka.Server.Configuration;

namespace Lofka.Server.Storage;

public sealed class TopicStore
{
    private readonly ServerConfig _config;
    private readonly ConcurrentDictionary<string, TopicInfo> _topics = new();

    public TopicStore(ServerConfig config)
    {
        _config = config;
    }

    public TopicInfo? GetTopic(string name)
    {
        _topics.TryGetValue(name, out var topic);
        return topic;
    }

    public IReadOnlyCollection<TopicInfo> GetAllTopics() => _topics.Values.ToList();

    public TopicInfo GetOrCreateTopic(string name, int? partitionCount = null)
    {
        return _topics.GetOrAdd(name, _ => new TopicInfo(name, Guid.NewGuid(), partitionCount ?? _config.DefaultPartitionCount));
    }

    public (TopicInfo? Topic, short ErrorCode) CreateTopic(string name, int partitionCount, bool failIfExists = false)
    {
        var topic = new TopicInfo(name, Guid.NewGuid(), partitionCount);
        if (_topics.TryAdd(name, topic))
            return (topic, 0);
        if (failIfExists)
            return (null, 36); // TOPIC_ALREADY_EXISTS
        return (_topics[name], 0);
    }

    public bool DeleteTopic(string name)
    {
        return _topics.TryRemove(name, out _);
    }
}

public sealed class TopicInfo
{
    public string Name { get; }
    public Guid TopicId { get; }
    public PartitionLog[] Partitions { get; }

    public TopicInfo(string name, Guid topicId, int partitionCount)
    {
        Name = name;
        TopicId = topicId;
        Partitions = new PartitionLog[partitionCount];
        for (int i = 0; i < partitionCount; i++)
            Partitions[i] = new PartitionLog(i);
    }
}
