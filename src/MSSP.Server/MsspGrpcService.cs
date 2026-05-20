using System.Text.RegularExpressions;
using Grpc.Core;
using Google.Protobuf;
using AppendRequest = MSSP.Grpc.AppendRequest;
using AppendResponse = MSSP.Grpc.AppendResponse;
using ReadRequest = MSSP.Grpc.ReadRequest;
using GrpcRecordedEvent = MSSP.Grpc.RecordedEvent;
using SubscribeRequest = MSSP.Grpc.SubscribeRequest;
using GrpcSubscriptionEvent = MSSP.Grpc.SubscriptionEvent;
using GrpcSubscriptionFilter = MSSP.Grpc.SubscriptionFilter;
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
        await foreach (var e in client.ReadAsync(new StreamId(request.StreamId), (StreamRevision)request.FromRevision, context.CancellationToken)) {
            await responseStream.WriteAsync(new GrpcRecordedEvent {
                StreamId = e.StreamId.Value,
                Revision = e.Revision,
                EventType = e.EventType,
                Data = ByteString.CopyFrom(e.Data.Span),
                TimestampNs = (e.Timestamp.UtcTicks - DateTimeOffset.UnixEpoch.Ticks) * 100L
            });
        }
    }

    /// <summary>
    /// Subscribes to events matching the specified filter, starting at the given global position.
    /// </summary>
    /// <param name="request">The subscribe request containing the filter and starting position.</param>
    /// <param name="responseStream">The server stream writer to which subscription events are written.</param>
    /// <param name="context">The gRPC server call context.</param>
    public override async Task Subscribe(SubscribeRequest request, IServerStreamWriter<GrpcSubscriptionEvent> responseStream, ServerCallContext context) {
        var filter = FromProto(request.Filter);
        await foreach (var e in client.SubscribeAsync(filter, new GlobalPosition(request.FromPosition), context.CancellationToken)) {
            await responseStream.WriteAsync(new GrpcSubscriptionEvent {
                StreamId    = e.StreamId.Value,
                Revision    = e.Revision,
                EventType   = e.EventType,
                Data        = ByteString.CopyFrom(e.Data.Span),
                TimestampNs = (e.Timestamp.UtcTicks - DateTimeOffset.UnixEpoch.Ticks) * 100L,
                Position    = e.Position.Value
            });
        }
    }

    static StreamRevision ToStreamRevision(long value) => value switch {
        -1 => StreamRevision.Any,
        -2 => StreamRevision.NoStream,
        -3 => StreamRevision.StreamExists,
        >= 0 => (StreamRevision)(ulong)value,
        _ => throw new RpcException(new Status(StatusCode.InvalidArgument, $"Invalid revision value: {value}."))
    };

    static SubscriptionFilter FromProto(GrpcSubscriptionFilter? f) {
        if (f is null) return SubscriptionFilter.All;
        return f.KindCase switch {
            GrpcSubscriptionFilter.KindOneofCase.All            => SubscriptionFilter.All,
            GrpcSubscriptionFilter.KindOneofCase.StreamIdFilter => SubscriptionFilter.ForStream(new StreamId(f.StreamIdFilter.StreamId)),
            GrpcSubscriptionFilter.KindOneofCase.StreamPrefix   => SubscriptionFilter.ForStreamPrefix(f.StreamPrefix.Prefix),
            GrpcSubscriptionFilter.KindOneofCase.StreamPattern  => SubscriptionFilter.ForStreamPattern(new Regex(f.StreamPattern.Pattern)),
            GrpcSubscriptionFilter.KindOneofCase.EventTypeFilter  => SubscriptionFilter.ForEventType(f.EventTypeFilter.EventType),
            GrpcSubscriptionFilter.KindOneofCase.EventTypePattern => SubscriptionFilter.ForEventTypePattern(new Regex(f.EventTypePattern.Pattern)),
            GrpcSubscriptionFilter.KindOneofCase.And            => f.And.Filters.Select(FromProto).Aggregate((a, b) => a.And(b)),
            _                                                   => SubscriptionFilter.All
        };
    }
}
