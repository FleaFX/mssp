namespace Log.Extensions;

internal static class ExtensionsForIAsyncEnumerable {
    public static async Task<IEnumerable<T>> EnumerateAsync<T>(this IAsyncEnumerable<T> asyncEnumerable) {
        var result = new List<T>();

        var enumerator = asyncEnumerable.GetAsyncEnumerator(new CancellationToken(true));
        while (await enumerator.MoveNextAsync()) {
            result.Add(enumerator.Current);
        }

        return result;
    }
}