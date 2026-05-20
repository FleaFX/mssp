using System.Text.RegularExpressions;

namespace MSSP;

/// <summary>
/// Determines which events are delivered to a subscription.
/// </summary>
public abstract class SubscriptionFilter {
    SubscriptionFilter() { }

    /// <summary>
    /// A filter that matches all events.
    /// </summary>
    public static readonly SubscriptionFilter All = new AllFilter();

    /// <summary>
    /// Returns a filter that matches events from a specific stream.
    /// </summary>
    public static SubscriptionFilter ForStream(StreamId id) => new StreamIdFilter(id);

    /// <summary>
    /// Returns a filter that matches events from streams whose name starts with <paramref name="prefix"/>.
    /// </summary>
    public static SubscriptionFilter ForStreamPrefix(string prefix) => new StreamPrefixFilter(prefix);

    /// <summary>
    /// Returns a filter that matches events from streams whose name matches <paramref name="pattern"/>.
    /// </summary>
    public static SubscriptionFilter ForStreamPattern(Regex pattern) => new StreamPatternFilter(pattern);

    /// <summary>
    /// Returns a filter that matches events with a specific event type.
    /// </summary>
    public static SubscriptionFilter ForEventType(string eventType) => new EventTypeFilter(eventType);

    /// <summary>
    /// Returns a filter that matches events whose type matches <paramref name="pattern"/>.
    /// </summary>
    public static SubscriptionFilter ForEventTypePattern(Regex pattern) => new EventTypePatternFilter(pattern);

    /// <summary>
    /// Returns a filter that matches events matched by both this filter and <paramref name="other"/>.
    /// </summary>
    public SubscriptionFilter And(SubscriptionFilter other) => new AndFilter(this, other);

    /// <summary>
    /// Returns true if this filter matches <paramref name="evt"/>.
    /// </summary>
    public abstract bool Matches(SubscriptionEvent evt);

    sealed class AllFilter : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => true;
    }

    sealed class StreamIdFilter(StreamId id) : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => evt.StreamId.Value == id.Value;
    }

    sealed class StreamPrefixFilter(string prefix) : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => evt.StreamId.Value.StartsWith(prefix, StringComparison.Ordinal);
    }

    sealed class StreamPatternFilter(Regex pattern) : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => pattern.IsMatch(evt.StreamId.Value);
    }

    sealed class EventTypeFilter(string eventType) : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => evt.EventType == eventType;
    }

    sealed class EventTypePatternFilter(Regex pattern) : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => pattern.IsMatch(evt.EventType);
    }

    sealed class AndFilter(SubscriptionFilter left, SubscriptionFilter right) : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => left.Matches(evt) && right.Matches(evt);
    }
}
