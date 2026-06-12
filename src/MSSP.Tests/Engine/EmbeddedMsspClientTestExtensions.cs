namespace MSSP.Engine;

internal static class EmbeddedMsspClientTestExtensions {
    internal static async Task WaitForMaintenanceIdleAsync(this EmbeddedMsspClient client, CancellationToken cancellationToken = default) {
        while (!client.IsMaintenanceIdle)
            await Task.Delay(10, cancellationToken);
    }
}
