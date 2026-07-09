namespace Hellnet.Kafka.Abstractions;

/// <summary>
/// Metadata for a consumed message.
/// </summary>
public interface IMessageContext
{
    public string MessageId { get; }
    public string Topic { get; }
    public DateTime Timestamp { get; }
    public int Partition { get; }
    public long Offset { get; }
    public int DeliveryAttempt { get; }
    public IReadOnlyDictionary<string, string> Headers { get; }
}
