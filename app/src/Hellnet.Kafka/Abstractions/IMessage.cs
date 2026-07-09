namespace Hellnet.Kafka.Abstractions;

/// <summary>
/// Marker interface for all Kafka messages.
/// </summary>
public interface IMessage
{
    /// <summary>
    /// Logical message type identifier used for topic routing and schema resolution.
    /// Convention: "{domain}.{action}.v{version}" (e.g., "order.created.v1").
    /// </summary>
    string MessageType { get; }
}
