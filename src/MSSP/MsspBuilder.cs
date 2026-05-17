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
}
