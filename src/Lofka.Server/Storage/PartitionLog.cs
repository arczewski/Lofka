using System.Buffers.Binary;

namespace Lofka.Server.Storage;

public sealed class PartitionLog
{
    private readonly int _partitionIndex;
    private readonly List<RecordBatchEntry> _batches = new();
    private readonly ReaderWriterLockSlim _lock = new();
    private long _nextOffset;
    private TaskCompletionSource _newDataSignal = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public PartitionLog(int partitionIndex)
    {
        _partitionIndex = partitionIndex;
    }

    public int PartitionIndex => _partitionIndex;
    public long NextOffset
    {
        get
        {
            _lock.EnterReadLock();
            try { return _nextOffset; }
            finally { _lock.ExitReadLock(); }
        }
    }

    /// <summary>
    /// Appends a record batch. Patches the baseOffset in the raw bytes.
    /// Returns the assigned base offset.
    /// </summary>
    public long Append(byte[] batchBytes)
    {
        _lock.EnterWriteLock();
        try
        {
            long baseOffset = _nextOffset;

            // Patch baseOffset (first 8 bytes of the batch)
            BinaryPrimitives.WriteInt64BigEndian(batchBytes.AsSpan(0, 8), baseOffset);

            // Read recordsCount from the batch header (offset 57, int32)
            int recordsCount = BinaryPrimitives.ReadInt32BigEndian(batchBytes.AsSpan(57, 4));
            _nextOffset += recordsCount;

            _batches.Add(new RecordBatchEntry(baseOffset, recordsCount, batchBytes));

            // Signal waiting fetch requests
            var oldSignal = _newDataSignal;
            _newDataSignal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            oldSignal.TrySetResult();

            return baseOffset;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Reads batches starting from the given offset, up to maxBytes.
    /// </summary>
    public (List<byte[]> Batches, long HighWatermark) Read(long fromOffset, int maxBytes)
    {
        _lock.EnterReadLock();
        try
        {
            var result = new List<byte[]>();
            int totalBytes = 0;

            foreach (var entry in _batches)
            {
                // Skip batches entirely before the requested offset
                if (entry.BaseOffset + entry.RecordCount <= fromOffset)
                    continue;

                if (totalBytes + entry.Data.Length > maxBytes && result.Count > 0)
                    break;

                result.Add(entry.Data);
                totalBytes += entry.Data.Length;
            }

            return (result, _nextOffset);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Waits for new data to be appended, with timeout.
    /// </summary>
    public async Task WaitForDataAsync(int timeoutMs, CancellationToken ct)
    {
        Task signal;
        _lock.EnterReadLock();
        try
        {
            signal = _newDataSignal.Task;
        }
        finally
        {
            _lock.ExitReadLock();
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeoutMs);

        try
        {
            await signal.WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
            // Timeout or cancellation — normal for fetch long-poll
        }
    }
}

public sealed record RecordBatchEntry(long BaseOffset, int RecordCount, byte[] Data);
