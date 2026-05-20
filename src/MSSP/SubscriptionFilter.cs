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

    /// <summary>
    /// Matches all events.
    /// </summary>
    public sealed class AllFilter : SubscriptionFilter {
        public override bool Matches(SubscriptionEvent evt) => true;
    }

    /// <summary>
    /// Matches events from a specific stream.
    /// </summary>
    public sealed class StreamIdFilter(StreamId id) : SubscriptionFilter {
        /// <summary>The stream to match.</summary>
        public StreamId Id => id;
        public override bool Matches(SubscriptionEvent evt) => evt.StreamId.Value == id.Value;
    }

    /// <summary>
    /// Matches events from streams whose name starts with a given prefix.
    /// </summary>
    public sealed class StreamPrefixFilter(string prefix) : SubscriptionFilter {
        /// <summary>The prefix to match against stream names.</summary>
        public string Prefix => prefix;
        public override bool Matches(SubscriptionEvent evt) => evt.StreamId.Value.StartsWith(prefix, StringComparison.Ordinal);
    }

    /// <summary>
    /// Matches events from streams whose name matches a regular expression.
    /// </summary>
    public sealed class StreamPatternFilter(Regex pattern) : SubscriptionFilter {
        /// <summary>The pattern to match against stream names.</summary>
        public Regex Pattern => pattern;
        public override bool Matches(SubscriptionEvent evt) => pattern.IsMatch(evt.StreamId.Value);
    }

    /// <summary>
    /// Matches events with a specific event type.
    /// </summary>
    public sealed class EventTypeFilter(string eventType) : SubscriptionFilter {
        /// <summary>The event type to match.</summary>
        public string EventType => eventType;
        public override bool Matches(SubscriptionEvent evt) => evt.EventType == eventType;
    }

    /// <summary>
    /// Matches events whose type matches a regular expression.
    /// </summary>
    public sealed class EventTypePatternFilter(Regex pattern) : SubscriptionFilter {
        /// <summary>The pattern to match against event types.</summary>
        public Regex Pattern => pattern;
        public override bool Matches(SubscriptionEvent evt) => pattern.IsMatch(evt.EventType);
    }

    /// <summary>
    /// Matches events that satisfy both constituent filters.
    /// </summary>
    public sealed class AndFilter(SubscriptionFilter left, SubscriptionFilter right) : SubscriptionFilter {
        /// <summary>The left operand.</summary>
        public SubscriptionFilter Left => left;
        /// <summary>The right operand.</summary>
        public SubscriptionFilter Right => right;
        public override bool Matches(SubscriptionEvent evt) => left.Matches(evt) && right.Matches(evt);
    }
}
