using Confluent.Kafka;

using Hellnet.Kafka.Abstractions;

namespace Hellnet.Kafka.Internal;

internal sealed class MessageContext : IMessageContext
{
    private int? _deliveryAttempt;

    public MessageContext(
        string messageId,
        string topic,
        DateTime timestamp,
        Partition partition,
        Offset offset,
        Headers? headers)
    {
        MessageId = messageId;
        Topic = topic;
        Timestamp = timestamp;
        Partition = partition.Value;
        Offset = offset.Value;
        Headers = headers?
            .ToDictionary(h => h.Key, h => h.GetValueBytes() is { } bytes
                ? System.Text.Encoding.UTF8.GetString(bytes) : string.Empty)
            ?? new Dictionary<string, string>();

        if (Headers.TryGetValue("delivery.attempt", out var attemptStr)
            && int.TryParse(attemptStr, out var attempt))
        {
            _deliveryAttempt = attempt;
        }
    }

    public string MessageId { get; }
    public string Topic { get; }
    public DateTime Timestamp { get; }
    public int Partition { get; }
    public long Offset { get; }
    public int DeliveryAttempt => _deliveryAttempt ?? 0;
    public IReadOnlyDictionary<string, string> Headers { get; }
}
