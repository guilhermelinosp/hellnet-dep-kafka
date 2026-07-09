using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Polly;

namespace Hellnet.Kafka.Serialization;

/// <summary>
/// Avro serializer using Confluent AvroSerdes with Schema Registry.
/// Wrapped with Polly resilience pipeline for registry calls.
/// </summary>
public sealed class AvroMessageSerializer : IMessageSerializer
{
    private readonly ISchemaRegistryClient _registry;
    private readonly ResiliencePipeline _pipeline;

    public AvroMessageSerializer(ISchemaRegistryClient registry)
        : this(registry, null)
    {
    }

    internal AvroMessageSerializer(ISchemaRegistryClient registry, HellnetKafkaOptions? options)
    {
        _registry = registry;
        _pipeline = options is not null
            ? ResiliencePipelines.SchemaRegistry(options)
            : ResiliencePipelines.SchemaRegistry(new HellnetKafkaOptions());
    }

    public byte[] Serialize<TMessage>(TMessage message)
        where TMessage : class, IMessage
    {
        return _pipeline.Execute(() =>
        {
            var serializer = new AvroSerializer<TMessage>(_registry);
            return serializer.SerializeAsync(message, SerializationContext.Empty)
                .GetAwaiter().GetResult();
        });
    }

    public TMessage Deserialize<TMessage>(byte[] data)
        where TMessage : class, IMessage
    {
        return _pipeline.Execute(() =>
        {
            var deserializer = new AvroDeserializer<TMessage>(_registry);
            return deserializer.DeserializeAsync(data, false, SerializationContext.Empty)
                .GetAwaiter().GetResult();
        });
    }
}
