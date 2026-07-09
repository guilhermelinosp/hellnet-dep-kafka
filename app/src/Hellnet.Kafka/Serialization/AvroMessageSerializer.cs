using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Hellnet.Kafka.Abstractions;

namespace Hellnet.Kafka.Serialization;

/// <summary>
/// Avro serializer using Confluent AvroSerdes with Schema Registry.
/// For use with DefaultSerializer=avro. Requires SchemaRegistryUrl in options.
/// </summary>
public sealed class AvroMessageSerializer : IMessageSerializer
{
    private readonly ISchemaRegistryClient _registry;

    public AvroMessageSerializer(ISchemaRegistryClient registry)
    {
        _registry = registry;
    }

    public byte[] Serialize<TMessage>(TMessage message)
        where TMessage : class, IMessage
    {
        var serializer = new AvroSerializer<TMessage>(_registry);
        return serializer.SerializeAsync(message, SerializationContext.Empty)
            .GetAwaiter().GetResult();
    }

    public TMessage Deserialize<TMessage>(byte[] data)
        where TMessage : class, IMessage
    {
        var deserializer = new AvroDeserializer<TMessage>(_registry);
        return deserializer.DeserializeAsync(data, false, SerializationContext.Empty)
            .GetAwaiter().GetResult();
    }
}
