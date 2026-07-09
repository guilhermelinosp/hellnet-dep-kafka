using System.Diagnostics.CodeAnalysis;

namespace Hellnet.Kafka.Abstractions;

/// <summary>
/// Configures how an <see cref="IMessageHandler{TMessage}"/> consumes messages.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = true)]
[ExcludeFromCodeCoverage]
public sealed class MessageHandlerAttribute : Attribute
{
    /// <summary>
    /// Topic to consume from. If empty, resolved from message type.
    /// </summary>
    public string? Topic { get; set; }

    /// <summary>
    /// Consumer group. If empty, defaults to service name from options.
    /// </summary>
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// Max retry attempts before sending to dead-letter topic. Default: 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;
}
