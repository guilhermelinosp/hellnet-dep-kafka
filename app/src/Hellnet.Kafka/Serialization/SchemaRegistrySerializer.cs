using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;

using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;

namespace Hellnet.Kafka.Serialization;

/// <summary>
/// Serializer that uses Confluent Schema Registry (Avro, JSON, Protobuf).
/// Registers/fetches schemas automatically from the Apicurio Registry.
/// Used when HELLNET_KAFKA_SCHEMA_REGISTRY_URL is configured.
/// </summary>
internal sealed class SchemaRegistrySerializer : IMessageSerializer, IDisposable
{
    private readonly ISchemaRegistryClient _client;
    private readonly string _format;
    private readonly ConcurrentDictionary<Type, object> _serializers = new();
    private readonly ConcurrentDictionary<Type, object> _deserializers = new();

    [ExcludeFromCodeCoverage]
    public SchemaRegistrySerializer(HellnetKafkaOptions options)
    {
        _format = options.DefaultSerializer;

        var config = new SchemaRegistryConfig
        {
            Url = options.SchemaRegistryUrl,
        };

        if (!string.IsNullOrWhiteSpace(options.SaslUsername))
        {
            config.BasicAuthUserInfo = $"{options.SaslUsername}:{options.SaslPassword}";
        }

        _client = new CachedSchemaRegistryClient(config);
    }

    internal SchemaRegistrySerializer(ISchemaRegistryClient client, string format)
    {
        _client = client;
        _format = format;
    }

    [ExcludeFromCodeCoverage]
    public byte[] Serialize<TMessage>(TMessage message)
        where TMessage : class, IMessage
    {
        var serializer = (ISerializer<TMessage>)_serializers.GetOrAdd(typeof(TMessage), key =>
        {
            var serializerType = _format switch
            {
                "avro" => typeof(AvroSerializer<>).MakeGenericType(key),
                "protobuf" => typeof(ProtobufSerializer<>).MakeGenericType(key),
                _ => typeof(JsonSerializer<>).MakeGenericType(key),
            };
            return Activator.CreateInstance(serializerType, _client)!;
        });

        var context = new SerializationContext(MessageComponentType.Value, message.MessageType);
        return serializer.Serialize(message, context);
    }

    [ExcludeFromCodeCoverage]
    public TMessage Deserialize<TMessage>(byte[] data)
        where TMessage : class, IMessage
    {
        var deserializer = (IDeserializer<TMessage>)_deserializers.GetOrAdd(typeof(TMessage), key =>
        {
            var deserializerType = _format switch
            {
                "avro" => typeof(AvroDeserializer<>).MakeGenericType(key),
                "protobuf" => typeof(ProtobufDeserializer<>).MakeGenericType(key),
                _ => typeof(JsonDeserializer<>).MakeGenericType(key),
            };
            return Activator.CreateInstance(deserializerType, _client)!;
        });

        var context = new SerializationContext(MessageComponentType.Value, typeof(TMessage).Name);
        return deserializer.Deserialize(data, data.Length != 0, context);
    }

    public void Dispose()
    {
        _client?.Dispose();
    }
}
