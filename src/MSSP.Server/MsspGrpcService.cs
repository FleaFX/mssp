using Grpc.Core;
using Google.Protobuf;
using MSSP.Raft;
using AppendRequest = MSSP.Grpc.AppendRequest;
using AppendResponse = MSSP.Grpc.AppendResponse;
using ReadRequest = MSSP.Grpc.ReadRequest;
using GrpcEventData = MSSP.Grpc.EventData;
using GrpcRecordedEvent = MSSP.Grpc.RecordedEvent;
using MsspBase = MSSP.Grpc.Mssp.MsspBase;

namespace MSSP.Server;

/// <summary>
/// gRPC service implementation that exposes an <see cref="IMsspClient"/> over the network.
/// </summary>
public sealed class MsspGrpcService(IMsspClient client) : MsspBase {
    /// <summary>
    /// Appends events to the specified stream.
    /// </summary>
    /// <param name="request">The append request containing the stream id, expected revision and events to append.</param>
    /// <param name="context">The gRPC server call context.</param>
    /// <returns>An empty response on success.</returns>
    /// <exception cref="RpcException">
    /// Thrown with <see cref="StatusCode.FailedPrecondition"/> when the stream's current revision does not match
    /// the expected revision, or with <see cref="StatusCode.InvalidArgument"/> when the revision value is invalid.
    /// </exception>
    public override async Task<AppendResponse> Append(AppendRequest request, ServerCallContext context) {
        if (string.IsNullOrEmpty(request.StreamId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "stream_id is required."));
        try {
            await client.AppendAsync(
                new StreamId(request.StreamId),
                ToStreamRevision(request.ExpectedRevision),
                request.Events.Select(e => new EventData(e.EventType, e.Data.Memory)),
                context.CancellationToken);
            return new AppendResponse();
        } catch (OptimisticConcurrencyException ex) {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        } catch (NotLeaderException ex) {
            var metadata = new Metadata();
            if (ex.LeaderHint is not null)
                metadata.Add("leader-hint", ex.LeaderHint);
            throw new RpcException(new Status(StatusCode.Unavailable, "Not the leader."), metadata);
        }
    }

    /// <summary>
    /// Reads events from the specified stream, starting at the given revision.
    /// </summary>
    /// <param name="request">The read request containing the stream id and starting revision.</param>
    /// <param name="responseStream">The server stream writer to which recorded events are written in order.</param>
    /// <param name="context">The gRPC server call context.</param>
    public override async Task Read(ReadRequest request, IServerStreamWriter<GrpcRecordedEvent> responseStream, ServerCallContext context) {
        if (string.IsNullOrEmpty(request.StreamId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "stream_id is required."));
        try {
            await foreach (var e in client.ReadAsync(new StreamId(request.StreamId), (StreamRevision)request.FromRevision, context.CancellationToken)) {
                await responseStream.WriteAsync(new GrpcRecordedEvent {
                    StreamId = e.StreamId.Value,
                    Revision = e.Revision,
                    EventType = e.EventType,
                    Data = ByteString.CopyFrom(e.Data.Span),
                    TimestampNs = (e.Timestamp.UtcTicks - DateTimeOffset.UnixEpoch.Ticks) * 100L
                });
            }
        } catch (NotLeaderException ex) {
            var metadata = new Metadata();
            if (ex.LeaderHint is not null)
                metadata.Add("leader-hint", ex.LeaderHint);
            throw new RpcException(new Status(StatusCode.Unavailable, "Not the leader."), metadata);
        }
    }

    static StreamRevision ToStreamRevision(long value) => value switch {
        -1 => StreamRevision.Any,
        -2 => StreamRevision.NoStream,
        -3 => StreamRevision.StreamExists,
        >= 0 => (StreamRevision)(ulong)value,
        _ => throw new RpcException(new Status(StatusCode.InvalidArgument, $"Invalid revision value: {value}."))
    };
}
