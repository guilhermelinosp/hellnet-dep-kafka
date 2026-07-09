using Hellnet.Kafka.Abstractions;

namespace Hellnet.Kafka.Serialization;

/// <summary>
/// Pluggable serializer for Kafka message payloads.
/// </summary>
public interface IMessageSerializer
{
    /// <summary>
    /// Serializes a message to a byte array.
    /// </summary>
    byte[] Serialize<TMessage>(TMessage message)
        where TMessage : IMessage;

    /// <summary>
    /// Deserializes a byte array to a message.
    /// </summary>
    TMessage Deserialize<TMessage>(byte[] data)
        where TMessage : IMessage;
}
