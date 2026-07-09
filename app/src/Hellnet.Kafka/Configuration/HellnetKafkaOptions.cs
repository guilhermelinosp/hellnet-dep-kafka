namespace Hellnet.Kafka.Configuration;

/// <summary>
/// Options for Hellnet.Kafka. Populated from environment variables.
/// All properties map to HELLNET_KAFKA_* env vars for consistency.
/// </summary>
public sealed class HellnetKafkaOptions
{
    /// <summary>Comma-separated broker list. Env: HELLNET_KAFKA_BROKERS. Default: localhost:9092.</summary>
    public string Brokers { get; init; } = "localhost:9092";

    /// <summary>Consumer group ID. Env: HELLNET_KAFKA_CONSUMER_GROUP. Default: service name.</summary>
    public string ConsumerGroup { get; init; } = string.Empty;

    /// <summary>Default serializer: json, avro, protobuf. Env: HELLNET_KAFKA_DEFAULT_SERIALIZER. Default: json.</summary>
    public string DefaultSerializer { get; init; } = "json";

    /// <summary>Schema Registry URL. Env: HELLNET_KAFKA_SCHEMA_REGISTRY_URL. Optional.</summary>
    public string? SchemaRegistryUrl { get; init; }

    /// <summary>Auto-discover and register IMessageHandler implementations. Env: HELLNET_KAFKA_AUTO_REGISTER_HANDLERS. Default: true.</summary>
    public bool AutoRegisterHandlers { get; init; } = true;

    /// <summary>Max retries before dead-letter. Env: HELLNET_KAFKA_MAX_RETRIES. Default: 3.</summary>
    public int MaxRetries { get; init; } = 3;

    /// <summary>Enable idempotent producer. Env: HELLNET_KAFKA_IDEMPOTENT. Default: true.</summary>
    public bool Idempotent { get; init; } = true;

    /// <summary>Required acks: all, leader, none. Env: HELLNET_KAFKA_ACKS. Default: all.</summary>
    public string Acks { get; init; } = "all";

    /// <summary>Auto offset reset: earliest, latest, none. Env: HELLNET_KAFKA_AUTO_OFFSET_RESET. Default: earliest.</summary>
    public string AutoOffsetReset { get; init; } = "earliest";

    /// <summary>Client ID. Env: HELLNET_KAFKA_CLIENT_ID. Default: machine name.</summary>
    public string ClientId { get; init; } = Environment.MachineName;

    // SASL / SSL — optional

    /// <summary>SASL mechanism. Env: HELLNET_KAFKA_SASL_MECHANISM. e.g., PLAIN, SCRAM-SHA-512.</summary>
    public string? SaslMechanism { get; init; }

    /// <summary>SASL username. Env: HELLNET_KAFKA_SASL_USERNAME.</summary>
    public string? SaslUsername { get; init; }

    /// <summary>SASL password. Env: HELLNET_KAFKA_SASL_PASSWORD.</summary>
    public string? SaslPassword { get; init; }

    /// <summary>Topic to send dead-letter messages. Env: HELLNET_KAFKA_DEAD_LETTER_TOPIC. Default: {topic}.dlq.</summary>
    public string? DeadLetterTopic { get; init; }
}
