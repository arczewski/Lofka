using System.Collections.Concurrent;

namespace Lofka.Server.Storage;

public sealed class ConsumerGroupManager
{
    private readonly ConcurrentDictionary<string, ConsumerGroupState> _groups = new();

    public ConsumerGroupState GetOrCreateGroup(string groupId)
    {
        return _groups.GetOrAdd(groupId, _ => new ConsumerGroupState(groupId));
    }

    public ConsumerGroupState? GetGroup(string groupId)
    {
        _groups.TryGetValue(groupId, out var group);
        return group;
    }
}
