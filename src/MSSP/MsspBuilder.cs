using System.ComponentModel;
using Microsoft.Extensions.DependencyInjection;

namespace MSSP;

/// <summary>
/// A builder for configuring MSSP services on an <see cref="IServiceCollection"/>.
/// </summary>
public sealed class MsspBuilder(IServiceCollection services) {
    /// <summary>
    /// Gets the <see cref="IServiceCollection"/> to which MSSP services are added.
    /// </summary>
    public IServiceCollection Services { get; } = services;

    /// <summary>
    /// The <see cref="IHostedService"/> descriptor registered by <c>AddMssp()</c> for the
    /// embedded store. Used internally by <c>AddCluster()</c> to replace the embedded
    /// registrations with their cluster equivalents. Not intended for direct use.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public ServiceDescriptor? EmbeddedHostedServiceDescriptor { get; set; }
}
