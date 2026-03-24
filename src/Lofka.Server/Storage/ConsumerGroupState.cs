namespace Lofka.Server.Storage;

public enum GroupStatus
{
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

public sealed class ConsumerGroupMember
{
    public string MemberId { get; init; } = string.Empty;
    public string? GroupInstanceId { get; init; }
    public string ClientId { get; init; } = string.Empty;
    public string ClientHost { get; init; } = string.Empty;
    public List<string> Protocols { get; init; } = new();
    public byte[] ProtocolMetadata { get; init; } = Array.Empty<byte>();
    public byte[]? Assignment { get; set; }
    public TaskCompletionSource<JoinGroupResult>? JoinFuture { get; set; }
    public TaskCompletionSource<byte[]>? SyncFuture { get; set; }
    public DateTime LastHeartbeat { get; set; } = DateTime.UtcNow;
}

public sealed record JoinGroupResult(
    short ErrorCode,
    int GenerationId,
    string ProtocolName,
    string LeaderId,
    string MemberId,
    List<(string MemberId, byte[] Metadata)> Members);

public sealed class ConsumerGroupState
{
    private readonly object _lock = new();
    private readonly Dictionary<string, ConsumerGroupMember> _members = new();
    private int _nextMemberId;

    public string GroupId { get; }
    public GroupStatus Status { get; private set; } = GroupStatus.Empty;
    public int GenerationId { get; private set; }
    public string? LeaderId { get; private set; }
    public string ProtocolName { get; private set; } = string.Empty;

    public ConsumerGroupState(string groupId)
    {
        GroupId = groupId;
    }

    public async Task<JoinGroupResult> JoinAsync(
        string memberId, string? groupInstanceId, string clientId, string clientHost,
        List<string> protocols, byte[] protocolMetadata, int sessionTimeoutMs,
        CancellationToken ct)
    {
        TaskCompletionSource<JoinGroupResult> future;
        bool shouldComplete;

        lock (_lock)
        {
            // Assign member ID if new
            if (string.IsNullOrEmpty(memberId))
            {
                memberId = $"{clientId}-{Interlocked.Increment(ref _nextMemberId):x8}";
            }

            var member = new ConsumerGroupMember
            {
                MemberId = memberId,
                GroupInstanceId = groupInstanceId,
                ClientId = clientId,
                ClientHost = clientHost,
                Protocols = protocols,
                ProtocolMetadata = protocolMetadata,
                LastHeartbeat = DateTime.UtcNow,
            };

            _members[memberId] = member;

            // Trigger rebalance
            Status = GroupStatus.PreparingRebalance;
            GenerationId++;

            future = new TaskCompletionSource<JoinGroupResult>(TaskCreationOptions.RunContinuationsAsynchronously);
            member.JoinFuture = future;

            // For simplicity in dev emulator: complete rebalance immediately
            // (no waiting for additional members)
            shouldComplete = true;
        }

        if (shouldComplete)
        {
            // Small delay to allow other members joining concurrently
            try { await Task.Delay(100, ct); } catch (OperationCanceledException) { }
            CompleteJoin();
        }

        return await future.Task;
    }

    private void CompleteJoin()
    {
        lock (_lock)
        {
            if (Status != GroupStatus.PreparingRebalance) return;

            Status = GroupStatus.CompletingRebalance;

            // First member is the leader
            LeaderId = null;
            var memberList = new List<(string MemberId, byte[] Metadata)>();

            foreach (var m in _members.Values)
            {
                LeaderId ??= m.MemberId;
                memberList.Add((m.MemberId, m.ProtocolMetadata));
            }

            // Pick the first common protocol
            if (_members.Count > 0)
            {
                ProtocolName = _members.Values.First().Protocols.FirstOrDefault() ?? "range";
            }

            // Complete all pending JoinGroup futures
            foreach (var m in _members.Values)
            {
                var isLeader = m.MemberId == LeaderId;
                var result = new JoinGroupResult(
                    ErrorCode: 0,
                    GenerationId: GenerationId,
                    ProtocolName: ProtocolName,
                    LeaderId: LeaderId!,
                    MemberId: m.MemberId,
                    Members: isLeader ? memberList : new List<(string, byte[])>()
                );

                m.JoinFuture?.TrySetResult(result);
                m.JoinFuture = null;

                // Prepare SyncGroup future
                m.SyncFuture = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
    }

    public async Task<(short ErrorCode, byte[] Assignment)> SyncAsync(
        string memberId, int generationId,
        List<(string MemberId, byte[] Assignment)>? assignments,
        CancellationToken ct)
    {
        TaskCompletionSource<byte[]>? future;

        lock (_lock)
        {
            if (!_members.TryGetValue(memberId, out var member))
                return (25, Array.Empty<byte>()); // UNKNOWN_MEMBER_ID

            if (generationId != GenerationId)
                return (22, Array.Empty<byte>()); // ILLEGAL_GENERATION

            // If this member is the leader, distribute assignments
            if (assignments != null && assignments.Count > 0)
            {
                foreach (var (mid, assignment) in assignments)
                {
                    if (_members.TryGetValue(mid, out var target))
                    {
                        target.Assignment = assignment;
                        target.SyncFuture?.TrySetResult(assignment);
                    }
                }
                Status = GroupStatus.Stable;
            }

            future = member.SyncFuture;
        }

        if (future == null)
            return (0, Array.Empty<byte>());

        try
        {
            var assignment = await future.Task.WaitAsync(ct);
            return (0, assignment);
        }
        catch (OperationCanceledException)
        {
            return (27, Array.Empty<byte>()); // REBALANCE_IN_PROGRESS
        }
    }

    public short Heartbeat(string memberId, int generationId)
    {
        lock (_lock)
        {
            if (!_members.TryGetValue(memberId, out var member))
                return 25; // UNKNOWN_MEMBER_ID
            if (generationId != GenerationId)
                return 22; // ILLEGAL_GENERATION
            if (Status == GroupStatus.PreparingRebalance || Status == GroupStatus.CompletingRebalance)
                return 27; // REBALANCE_IN_PROGRESS

            member.LastHeartbeat = DateTime.UtcNow;
            return 0;
        }
    }

    public void Leave(string memberId)
    {
        lock (_lock)
        {
            _members.Remove(memberId);
            if (_members.Count == 0)
            {
                Status = GroupStatus.Empty;
                LeaderId = null;
            }
        }
    }
}
