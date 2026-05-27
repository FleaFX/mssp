using System.Collections.Concurrent;

namespace MSSP.Raft;

/// <summary>
/// An <see cref="IRaftTransport"/> that records every outbound RPC as a queued item and
/// suspends the caller until a test explicitly resolves the corresponding
/// <see cref="TaskCompletionSource{T}"/>. Used in unit tests that need to inspect or control
/// peer responses without real network I/O.
/// </summary>
internal sealed class RecordingRaftTransport : IRaftTransport {
    /// <summary>
    /// Outbound <see cref="VoteRequest"/> calls, in order of arrival.
    /// Resolve <c>Reply</c> to unblock the background task awaiting the RPC.
    /// </summary>
    public ConcurrentQueue<(string PeerId, VoteRequest Request, TaskCompletionSource<VoteResponse> Reply)>
        VoteRequests { get; } = new();

    /// <summary>
    /// Outbound <see cref="AppendEntriesRequest"/> calls, in order of arrival.
    /// Resolve <c>Reply</c> to unblock the background task awaiting the RPC.
    /// </summary>
    public ConcurrentQueue<(string PeerId, AppendEntriesRequest Request, TaskCompletionSource<AppendEntriesResponse> Reply)>
        AppendRequests { get; } = new();

    /// <summary>
    /// Outbound <see cref="InstallSnapshotRequest"/> calls, in order of arrival.
    /// Resolve <c>Reply</c> to unblock the background task awaiting the RPC.
    /// </summary>
    public ConcurrentQueue<(string PeerId, InstallSnapshotRequest Request, TaskCompletionSource<InstallSnapshotResponse> Reply)>
        SnapshotRequests { get; } = new();

    /// <inheritdoc/>
    public ValueTask<VoteResponse> RequestVoteAsync(string peerId, VoteRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<VoteResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
        VoteRequests.Enqueue((peerId, request, tcs));
        return new ValueTask<VoteResponse>(tcs.Task);
    }

    /// <inheritdoc/>
    public ValueTask<AppendEntriesResponse> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<AppendEntriesResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
        AppendRequests.Enqueue((peerId, request, tcs));
        return new ValueTask<AppendEntriesResponse>(tcs.Task);
    }

    /// <inheritdoc/>
    public ValueTask<InstallSnapshotResponse> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken cancellationToken = default) {
        var tcs = new TaskCompletionSource<InstallSnapshotResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
        SnapshotRequests.Enqueue((peerId, request, tcs));
        return new ValueTask<InstallSnapshotResponse>(tcs.Task);
    }
}
