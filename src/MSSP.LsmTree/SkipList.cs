using System.Collections;

namespace MSSP.LsmTree;

/// <summary>
/// An ordered, thread-safe dictionary implemented as a skip list.
/// Provides O(log n) expected time for search, insertion, and deletion.
/// </summary>
/// <typeparam name="TKey">The type of the key. Must be comparable.</typeparam>
/// <typeparam name="TValue">The type of the value.</typeparam>
/// <remarks>
/// Based on: William Pugh, "Skip Lists: A Probabilistic Alternative to Balanced Trees",
/// Communications of the ACM, June 1990.
/// </remarks>
sealed class SkipList<TKey, TValue> : IDisposable, IEnumerable<KeyValuePair<TKey, TValue>>
    where TKey : IComparable<TKey> {

    const int MaxLevel = 16;
    const double Probability = 0.5;

    readonly Node _head = new(MaxLevel);
    readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);

    // Reused across writes under the write lock — avoids a heap allocation per operation.
    readonly Node?[] _update = new Node?[MaxLevel];

    int _level = 1;
    volatile int _count;

    /// <summary>
    /// Gets the number of entries currently in the list.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Attempts to retrieve the value associated with the given <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="value">The value associated with <paramref name="key"/>, or <c>default</c> if not found.</param>
    /// <returns><c>true</c> if the key was found; otherwise <c>false</c>.</returns>
    public bool TryGet(TKey key, out TValue? value) {
        _lock.EnterReadLock();
        try {
            var node = FindNode(key);
            if (node != null) {
                value = node.Value;
                return true;
            }
            value = default;
            return false;
        } finally {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Inserts or updates a key/value pair.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="value">The value to associate with <paramref name="key"/>.</param>
    public void Write(TKey key, TValue value) {
        _lock.EnterWriteLock();
        try {
            var existing = FindWithUpdate(key);
            if (existing != null) {
                existing.Value = value;
                return;
            }

            var level = RandomLevel();
            if (level > _level) {
                for (var i = _level; i < level; i++)
                    _update[i] = _head;
                _level = level;
            }

            var node = new Node(key, value, level);
            for (var i = 0; i < level; i++) {
                node.Next[i] = _update[i]!.Next[i];
                _update[i]!.Next[i] = node;
            }

            _count++;
        } finally {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Removes the node with the given <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The key of the node to remove.</param>
    /// <returns><c>true</c> if the key was found and removed; otherwise <c>false</c>.</returns>
    public bool Delete(TKey key) {
        _lock.EnterWriteLock();
        try {
            var node = FindWithUpdate(key);
            if (node == null) return false;

            for (var i = 0; i < _level; i++) {
                if (_update[i]!.Next[i] != node) break;
                _update[i]!.Next[i] = node.Next[i];
            }

            while (_level > 1 && _head.Next[_level - 1] == null)
                _level--;

            _count--;
            return true;
        } finally {
            _lock.ExitWriteLock();
        }
    }

    /// <inheritdoc/>
    public void Dispose() =>
        _lock.Dispose();

    /// <summary>
    /// Returns an enumerator that yields all entries in ascending key order.
    /// </summary>
    /// <remarks>
    /// The read lock is held for the entire duration of the enumeration.
    /// Calling <see cref="Write"/> or <see cref="Delete"/> on the same instance from within
    /// the enumerating thread will deadlock. Calling <see cref="TryGet"/> will throw
    /// <see cref="LockRecursionException"/> because this instance uses
    /// <see cref="LockRecursionPolicy.NoRecursion"/>.
    /// </remarks>
    IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator() {
        _lock.EnterReadLock();
        try {
            var current = _head.Next[0];
            while (current != null) {
                yield return new(current.Key!, current.Value!);
                current = current.Next[0];
            }
        } finally {
            _lock.ExitReadLock();
        }
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() =>
        ((IEnumerable<KeyValuePair<TKey, TValue>>)this).GetEnumerator();

    // Must be called under read or write lock.
    Node? FindNode(TKey key) {
        var current = _head;
        for (var i = _level - 1; i >= 0; i--) {
            while (current.Next[i]?.Key!.CompareTo(key) < 0)
                current = current.Next[i]!;
        }
        var candidate = current.Next[0];
        return candidate?.Key!.CompareTo(key) == 0 ? candidate : null;
    }

    // Must be called under write lock. Also populates _update with the rightmost
    // predecessor at each level, which is needed to splice in a new node.
    Node? FindWithUpdate(TKey key) {
        var current = _head;
        for (var i = _level - 1; i >= 0; i--) {
            while (current.Next[i]?.Key!.CompareTo(key) < 0)
                current = current.Next[i]!;
            _update[i] = current;
        }
        var candidate = current.Next[0];
        return candidate?.Key!.CompareTo(key) == 0 ? candidate : null;
    }

    static int RandomLevel() {
        var level = 1;
        while (Random.Shared.NextDouble() < Probability && level < MaxLevel)
            level++;
        return level;
    }

    sealed class Node {
        internal readonly TKey? Key;
        internal TValue? Value;
        internal readonly Node?[] Next;

        internal Node(int maxLevel) => Next = new Node?[maxLevel];

        internal Node(TKey key, TValue value, int level) : this(level) {
            Key = key;
            Value = value;
        }
    }
}
