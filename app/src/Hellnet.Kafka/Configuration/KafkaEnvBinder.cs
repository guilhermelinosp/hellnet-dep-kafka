namespace Hellnet.Kafka.Configuration;

/// <summary>
/// Reads HellnetKafkaOptions from environment variables (HELLNET_KAFKA_*).
/// No appsettings.json, no IConfiguration — pure env-first.
/// </summary>
internal static class KafkaEnvBinder
{
    public static HellnetKafkaOptions Bind()
    {
        return new HellnetKafkaOptions
        {
            Brokers = Env("HELLNET_KAFKA_BROKERS", "localhost:9092"),
            ConsumerGroup = Env("HELLNET_KAFKA_CONSUMER_GROUP", Environment.GetEnvironmentVariable("HELLNET_SERVICE_NAME") ?? string.Empty),
            DefaultSerializer = Env("HELLNET_KAFKA_DEFAULT_SERIALIZER", "json"),
            SchemaRegistryUrl = EnvOrNull("HELLNET_KAFKA_SCHEMA_REGISTRY_URL"),
            AutoRegisterHandlers = EnvBool("HELLNET_KAFKA_AUTO_REGISTER_HANDLERS", true),
            MaxRetries = EnvInt("HELLNET_KAFKA_MAX_RETRIES", 3),
            Idempotent = EnvBool("HELLNET_KAFKA_IDEMPOTENT", true),
            Acks = Env("HELLNET_KAFKA_ACKS", "all"),
            AutoOffsetReset = Env("HELLNET_KAFKA_AUTO_OFFSET_RESET", "earliest"),
            ClientId = Env("HELLNET_KAFKA_CLIENT_ID", Environment.MachineName),
            SaslMechanism = EnvOrNull("HELLNET_KAFKA_SASL_MECHANISM"),
            SaslUsername = EnvOrNull("HELLNET_KAFKA_SASL_USERNAME"),
            SaslPassword = EnvOrNull("HELLNET_KAFKA_SASL_PASSWORD"),
            DeadLetterTopic = EnvOrNull("HELLNET_KAFKA_DEAD_LETTER_TOPIC"),
        };
    }

    internal static string Env(string key, string fallback)
        => Environment.GetEnvironmentVariable(key) ?? fallback;

    internal static string? EnvOrNull(string key)
        => Environment.GetEnvironmentVariable(key);

    internal static bool EnvBool(string key, bool fallback)
        => bool.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : fallback;

    internal static int EnvInt(string key, int fallback)
        => int.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : fallback;
}
