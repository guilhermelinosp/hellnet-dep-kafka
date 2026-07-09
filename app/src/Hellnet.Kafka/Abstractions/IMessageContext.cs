namespace Hellnet.Kafka.Abstractions;

/// <summary>
/// Metadata for a consumed message.
/// </summary>
public interface IMessageContext
{
    string MessageId { get; }
    string Topic { get; }
    DateTime Timestamp { get; }
    int Partition { get; }
    long Offset { get; }
    int DeliveryAttempt { get; }
    IReadOnlyDictionary<string, string> Headers { get; }
}
