using System.Diagnostics.CodeAnalysis;

namespace Hellnet.Kafka.Configuration;

/// <summary>
/// Reads HellnetKafkaOptions from environment variables (HELLNET_KAFKA_*).
/// No appsettings.json, no IConfiguration — pure env-first.
/// </summary>
internal static class KafkaEnvBinder
{
    /// <summary>
    /// Bind from env vars only (defaults apply when env var is not set).
    /// </summary>
    public static HellnetKafkaOptions Bind()
        => Bind(new HellnetKafkaOptions());

    /// <summary>
    /// Bind from env vars, starting from <paramref name="base"/> options.
    /// Env vars override base values (env-first).
    /// </summary>
    public static HellnetKafkaOptions Bind(HellnetKafkaOptions @base)
    {
        return new HellnetKafkaOptions
        {
            Brokers = Env("HELLNET_KAFKA_BROKERS", @base.Brokers),
            ConsumerGroup = Env("HELLNET_KAFKA_CONSUMER_GROUP",
                Environment.GetEnvironmentVariable("HELLNET_SERVICE_NAME") ?? @base.ConsumerGroup),
            ClientId = Env("HELLNET_KAFKA_CLIENT_ID", @base.ClientId),

            SaslMechanism = EnvOrNull("HELLNET_KAFKA_SASL_MECHANISM", @base.SaslMechanism),
            SaslUsername = EnvOrNull("HELLNET_KAFKA_SASL_USERNAME", @base.SaslUsername),
            SaslPassword = EnvOrNull("HELLNET_KAFKA_SASL_PASSWORD", @base.SaslPassword),
            SecurityProtocol = Env("HELLNET_KAFKA_SECURITY_PROTOCOL", @base.SecurityProtocol),
            SslCaLocation = EnvOrNull("HELLNET_KAFKA_SSL_CA_LOCATION", @base.SslCaLocation),
            SslEndpointIdentificationAlgorithm = Env("HELLNET_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", @base.SslEndpointIdentificationAlgorithm),

            DefaultSerializer = Env("HELLNET_KAFKA_DEFAULT_SERIALIZER", @base.DefaultSerializer),
            SchemaRegistryUrl = EnvOrNull("HELLNET_KAFKA_SCHEMA_REGISTRY_URL", @base.SchemaRegistryUrl),
            SchemaRegistryUsername = EnvOrNull("HELLNET_KAFKA_SCHEMA_REGISTRY_USERNAME", @base.SchemaRegistryUsername),
            SchemaRegistryPassword = EnvOrNull("HELLNET_KAFKA_SCHEMA_REGISTRY_PASSWORD", @base.SchemaRegistryPassword),

            TopicPrefix = Env("HELLNET_KAFKA_TOPIC_PREFIX", @base.TopicPrefix),
            GroupProtocol = Env("HELLNET_KAFKA_GROUP_PROTOCOL", @base.GroupProtocol),

            AutoRegisterHandlers = EnvBool("HELLNET_KAFKA_AUTO_REGISTER_HANDLERS", @base.AutoRegisterHandlers),
            MaxRetries = EnvInt("HELLNET_KAFKA_MAX_RETRIES", @base.MaxRetries),
            RetryDelayMs = EnvInt("HELLNET_KAFKA_RETRY_DELAY_MS", @base.RetryDelayMs),
            RetryMaxDelayMs = EnvInt("HELLNET_KAFKA_RETRY_MAX_DELAY_MS", @base.RetryMaxDelayMs),
            RetryJitterMs = EnvInt("HELLNET_KAFKA_RETRY_JITTER_MS", @base.RetryJitterMs),
            TimeoutProduceMs = EnvInt("HELLNET_KAFKA_TIMEOUT_PRODUCE_MS", @base.TimeoutProduceMs),
            TimeoutSchemaRegistryMs = EnvInt("HELLNET_KAFKA_TIMEOUT_SCHEMA_REGISTRY_MS", @base.TimeoutSchemaRegistryMs),
            CircuitBreakerFailureCount = EnvInt("HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT", @base.CircuitBreakerFailureCount),
            CircuitBreakerDurationMs = EnvInt("HELLNET_KAFKA_CIRCUIT_BREAKER_DURATION_MS", @base.CircuitBreakerDurationMs),
            Idempotent = EnvBool("HELLNET_KAFKA_IDEMPOTENT", @base.Idempotent),
            Acks = Env("HELLNET_KAFKA_ACKS", @base.Acks),
            AutoOffsetReset = Env("HELLNET_KAFKA_AUTO_OFFSET_RESET", @base.AutoOffsetReset),
            DeadLetterTopic = EnvOrNull("HELLNET_KAFKA_DEAD_LETTER_TOPIC", @base.DeadLetterTopic),
        };
    }

    internal static string Env(string key, string fallback)
        => Environment.GetEnvironmentVariable(key) ?? fallback;

    internal static string? EnvOrNull(string key, string? fallback)
        => Environment.GetEnvironmentVariable(key) ?? fallback;

    internal static bool EnvBool(string key, bool fallback)
        => bool.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : fallback;

    internal static int EnvInt(string key, int fallback)
        => int.TryParse(Environment.GetEnvironmentVariable(key), out var v) ? v : fallback;
}
