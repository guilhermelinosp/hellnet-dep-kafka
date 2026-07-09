namespace Hellnet.Kafka.Configuration;

/// <summary>
/// Preset de defaults para infra Hellnet.
/// Aplicado quando AddHellnetKafka(useInfraDefaults: true) é chamado.
/// Env vars HELLNET_KAFKA_* sobrescrevem qualquer default.
/// </summary>
internal static class HellnetKafkaDefaults
{
    public static HellnetKafkaOptions Create() => new()
    {
        // Broker
        Brokers = "kafka.hellnet.com.br:9094",

        // SASL/SSL
        SecurityProtocol = "sasl_ssl",
        SaslMechanism = "SCRAM-SHA-512",
        SaslUsername = "hellnet-app",
        SaslPassword = "hellnet2026",
        SslEndpointIdentificationAlgorithm = "",

        // Schema Registry
        DefaultSerializer = "avro",
        SchemaRegistryUrl = "https://schema.hellnet.com.br",

        // Topic
        TopicPrefix = "hellnet",

        // Behavior
        Acks = "all",
        AutoOffsetReset = "earliest",
        Idempotent = true,
        AutoRegisterHandlers = true,
        MaxRetries = 3,
        GroupProtocol = "classic",
    };
}
