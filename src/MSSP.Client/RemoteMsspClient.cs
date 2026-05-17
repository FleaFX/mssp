using System.Runtime.CompilerServices;
using Google.Protobuf;
using Grpc.Core;
using AppendRequest = MSSP.Grpc.AppendRequest;
using ReadRequest = MSSP.Grpc.ReadRequest;
using GrpcEventData = MSSP.Grpc.EventData;
using MsspClient = MSSP.Grpc.Mssp.MsspClient;

namespace MSSP.Client;

/// <summary>
/// <see cref="IMsspClient"/> implementation that communicates with a remote MSSP server over gRPC.
/// </summary>
sealed class RemoteMsspClient(MsspClient grpcClient) : IMsspClient {
    /// <inheritdoc/>
    public async ValueTask AppendAsync(StreamId streamId, StreamRevision expectedRevision, IEnumerable<EventData> events, CancellationToken ct = default) {
        var request = new AppendRequest {
            StreamId = streamId.Value,
            ExpectedRevision = (long)expectedRevision
        };
        foreach (var e in events)
            request.Events.Add(new GrpcEventData { EventType = e.EventType, Data = ByteString.CopyFrom(e.Data.Span) });
        try {
            await grpcClient.AppendAsync(request, cancellationToken: ct);
        } catch (RpcException ex) when (ex.StatusCode == StatusCode.FailedPrecondition) {
            throw new OptimisticConcurrencyException(streamId, expectedRevision);
        }
    }

    /// <inheritdoc/>
    public async IAsyncEnumerable<RecordedEvent> ReadAsync(StreamId streamId, StreamRevision from = default, [EnumeratorCancellation] CancellationToken ct = default) {
        var request = new ReadRequest { StreamId = streamId.Value, FromRevision = (ulong)(long)from };
        using var call = grpcClient.Read(request, cancellationToken: ct);
        while (await call.ResponseStream.MoveNext(ct)) {
            var e = call.ResponseStream.Current;
            yield return new RecordedEvent(
                new StreamId(e.StreamId),
                e.Revision,
                e.EventType,
                e.Data.Memory,
                new DateTimeOffset(DateTimeOffset.UnixEpoch.Ticks + e.TimestampNs / 100L, TimeSpan.Zero));
        }
    }
}
