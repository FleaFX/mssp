using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using MSSP.Engine;

namespace MSSP.Cluster;

/// <summary>
/// Extends <see cref="MsspBuilder"/> with an ASP.NET Core health check for the Raft cluster.
/// </summary>
public static class MsspClusterHealthCheckExtensions {
    /// <summary>
    /// Registers a readiness health check that reports the cluster's write availability:
    /// <c>Healthy</c> when a leader is known, <c>Degraded</c> during leader election,
    /// and <c>Unhealthy</c> before the Raft node has started.
    /// </summary>
    /// <remarks>
    /// Use this method instead of <c>AddHealthChecks()</c> from <c>MSSP.Engine</c>.
    /// The two checks are mutually exclusive: in cluster mode, <see cref="MsspHostedService"/>
    /// is not registered, so the embedded check cannot be resolved.
    /// </remarks>
    public static MsspBuilder AddClusterHealthChecks(this MsspBuilder builder) {
        builder.Services.AddHealthChecks()
            .AddCheck<ClusterHealthCheck>("mssp", tags: ["ready"]);
        return builder;
    }
}

/// <summary>
/// Health check that reports whether the MSSP cluster can currently accept writes.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item><c>Healthy</c> — this node is the leader, or the leader hint is known (writes are forwarded).</item>
/// <item><c>Degraded</c> — no leader is known; an election is in progress, writes are temporarily unavailable.</item>
/// <item><c>Unhealthy</c> — the Raft node has not started yet.</item>
/// </list>
/// <para>
/// Reads and subscriptions are always served locally and are not affected by leader availability.
/// </para>
/// </remarks>
internal sealed class ClusterHealthCheck(RaftHostedService raftService) : IHealthCheck {
    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default) {
        Raft.RaftNode node;
        try {
            node = raftService.Node;
        } catch (InvalidOperationException ex) {
            return Task.FromResult(HealthCheckResult.Unhealthy(ex.Message));
        }

        var data = new Dictionary<string, object> {
            ["term"]        = node.CurrentTerm,
            ["commitIndex"] = node.CommitIndex,
            ["isLeader"]    = node.IsLeader,
            ["leaderHint"]  = node.LeaderHint ?? "(unknown)",
        };

        return Task.FromResult(node switch {
            { IsLeader: true } => HealthCheckResult.Healthy("This node is the leader.", data),
            { LeaderHint: not null } => HealthCheckResult.Healthy($"Leader is node '{node.LeaderHint}'.", data),
            _ => HealthCheckResult.Degraded("No leader is currently known. Writes are temporarily unavailable.", null,  data)
        });
    }
}
