using System.Collections.Concurrent;

namespace Lofka.Server.Storage;

public sealed class OffsetStore
{
    // groupId -> (topic-partition -> offset)
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<(string Topic, int Partition), long>> _offsets = new();

    public void Commit(string groupId, string topic, int partition, long offset)
    {
        var groupOffsets = _offsets.GetOrAdd(groupId, _ => new ConcurrentDictionary<(string, int), long>());
        groupOffsets[(topic, partition)] = offset;
    }

    public long? GetOffset(string groupId, string topic, int partition)
    {
        if (!_offsets.TryGetValue(groupId, out var groupOffsets))
            return null;
        if (!groupOffsets.TryGetValue((topic, partition), out var offset))
            return null;
        return offset;
    }
}
