namespace Hellnet.Kafka.Abstractions;

/// <summary>
/// Handles a consumed message. Registered in DI and auto-discovered at startup.
/// </summary>
public interface IMessageHandler<TMessage>
    where TMessage : IMessage
{
    /// <summary>
    /// Process the message. Throw to trigger retry or dead-letter.
    /// </summary>
    Task HandleAsync(TMessage message, IMessageContext context, CancellationToken ct);
}
