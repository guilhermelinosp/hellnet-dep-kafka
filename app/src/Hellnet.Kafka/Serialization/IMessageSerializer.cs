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
    public byte[] Serialize<TMessage>(TMessage message)
        where TMessage : class, IMessage;

    /// <summary>
    /// Deserializes a byte array to a message.
    /// </summary>
    public TMessage Deserialize<TMessage>(byte[] data)
        where TMessage : class, IMessage;
}
