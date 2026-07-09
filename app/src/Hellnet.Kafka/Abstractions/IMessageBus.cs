namespace Hellnet.Kafka.Abstractions;

/// <summary>
/// Kafka producer abstraction. Publish messages without coupling to Confluent.Kafka.
/// </summary>
public interface IMessageBus
{
    /// <summary>
    /// Publishes a single message. Topic is resolved from message type or configuration.
    /// </summary>
    public Task PublishAsync<TMessage>(TMessage message, CancellationToken ct = default)
        where TMessage : class, IMessage;

    /// <summary>
    /// Publishes a batch of messages in a single transaction (if idempotent producer is enabled).
    /// </summary>
    public Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken ct = default)
        where TMessage : class, IMessage;
}
