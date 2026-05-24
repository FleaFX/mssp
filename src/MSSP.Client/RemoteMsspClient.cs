using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using AppendRequest = MSSP.Grpc.AppendRequest;
using ReadRequest = MSSP.Grpc.ReadRequest;
using GrpcEventData = MSSP.Grpc.EventData;
using SubscribeRequest = MSSP.Grpc.SubscribeRequest;
using GrpcReadDirection = MSSP.Grpc.ReadDirection;
using GrpcSubscriptionFilter = MSSP.Grpc.SubscriptionFilter;
using GrpcAllFilter = MSSP.Grpc.AllFilter;
using GrpcStreamIdFilter = MSSP.Grpc.StreamIdFilter;
using GrpcStreamPrefixFilter = MSSP.Grpc.StreamPrefixFilter;
using GrpcStreamPatternFilter = MSSP.Grpc.StreamPatternFilter;
using GrpcEventTypeFilter = MSSP.Grpc.EventTypeFilter;
using GrpcEventTypePatternFilter = MSSP.Grpc.EventTypePatternFilter;
using GrpcAndFilter = MSSP.Grpc.AndFilter;
using MsspClient = MSSP.Grpc.Mssp.MsspClient;

namespace MSSP.Client;

/// <summary>
/// <see cref="IMsspClient"/> implementation that communicates with a remote MSSP server over gRPC.
/// </summary>
sealed class RemoteMsspClient(MsspClient grpcClient) : IMsspClient {
    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken cancellationToken = default) {
        var request = new AppendRequest {
            StreamId = streamId.Value,
            ExpectedRevision = (long)expectedRevision
        };
        foreach (var e in events)
            request.Events.Add(new GrpcEventData { EventType = e.EventType, Data = ByteString.CopyFrom(e.Data.Span), Metadata = ByteString.CopyFrom(e.Metadata.Span) });
        try {
            await grpcClient.AppendAsync(request, cancellationToken: cancellationToken);
        } catch (RpcException ex) when (ex.StatusCode == StatusCode.FailedPrecondition) {
            throw new OptimisticConcurrencyException(streamId, expectedRevision);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, ReadDirection direction = ReadDirection.Forwards, long maxCount = long.MaxValue, [EnumeratorCancellation] CancellationToken cancellationToken = default) {
        var request = new ReadRequest { StreamId = streamId.Value, FromRevision = (ulong)(long)from, Direction = (GrpcReadDirection)direction, MaxCount = maxCount };
        using var call = grpcClient.Read(request, cancellationToken: cancellationToken);
        while (await call.ResponseStream.MoveNext(cancellationToken)) {
            var e = call.ResponseStream.Current;
            yield return new RecordedEvent(
                new StreamId(e.StreamId),
                e.Revision,
                e.EventType,
                e.Data.Memory,
                new DateTimeOffset(DateTimeOffset.UnixEpoch.Ticks + e.TimestampNs / 100L, TimeSpan.Zero),
                e.Metadata.Memory);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<SubscriptionEvent> SubscribeAsync(
        SubscriptionFilter filter,
        GlobalPosition fromPosition = default,
        [EnumeratorCancellation] CancellationToken cancellationToken = default) {

        var request = new SubscribeRequest { Filter = ToProto(filter), FromPosition = fromPosition.Value };
        using var call = grpcClient.Subscribe(request, cancellationToken: cancellationToken);
        while (await call.ResponseStream.MoveNext(cancellationToken)) {
            var e = call.ResponseStream.Current;
            yield return new SubscriptionEvent(
                new StreamId(e.StreamId),
                e.Revision,
                e.EventType,
                e.Data.Memory,
                new DateTimeOffset(DateTimeOffset.UnixEpoch.Ticks + e.TimestampNs / 100L, TimeSpan.Zero),
                new GlobalPosition(e.Position),
                e.Metadata.Memory);
        }
    }

    static GrpcSubscriptionFilter ToProto(SubscriptionFilter filter) => filter switch {
        SubscriptionFilter.AllFilter =>
            new GrpcSubscriptionFilter { All = new GrpcAllFilter() },
        SubscriptionFilter.StreamIdFilter f =>
            new GrpcSubscriptionFilter { StreamIdFilter = new GrpcStreamIdFilter { StreamId = f.Id.Value } },
        SubscriptionFilter.StreamPrefixFilter f =>
            new GrpcSubscriptionFilter { StreamPrefix = new GrpcStreamPrefixFilter { Prefix = f.Prefix } },
        SubscriptionFilter.StreamPatternFilter f =>
            new GrpcSubscriptionFilter { StreamPattern = new GrpcStreamPatternFilter { Pattern = f.Pattern.ToString() } },
        SubscriptionFilter.EventTypeFilter f =>
            new GrpcSubscriptionFilter { EventTypeFilter = new GrpcEventTypeFilter { EventType = f.EventType } },
        SubscriptionFilter.EventTypePatternFilter f =>
            new GrpcSubscriptionFilter { EventTypePattern = new GrpcEventTypePatternFilter { Pattern = f.Pattern.ToString() } },
        SubscriptionFilter.AndFilter f =>
            new GrpcSubscriptionFilter { And = new GrpcAndFilter { Filters = { ToProto(f.Left), ToProto(f.Right) } } },
        _ => new GrpcSubscriptionFilter { All = new GrpcAllFilter() }
    };
}
