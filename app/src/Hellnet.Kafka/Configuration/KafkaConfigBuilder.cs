using Confluent.Kafka;

namespace Hellnet.Kafka.Configuration;

/// <summary>
/// Builds Confluent.Kafka config from HellnetKafkaOptions.
/// Shared by producer, consumer, and DeadLetterService.
/// </summary>
internal static class KafkaConfigBuilder
{
    public static ProducerConfig BuildProducerConfig(HellnetKafkaOptions options, string? clientIdSuffix = null)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = options.Brokers,
            EnableIdempotence = options.Idempotent,
            Acks = options.Acks?.ToLowerInvariant() switch
            {
                "all" => Confluent.Kafka.Acks.All,
                "leader" => Confluent.Kafka.Acks.Leader,
                "none" => Confluent.Kafka.Acks.None,
                _ => Confluent.Kafka.Acks.All,
            },
            ClientId = string.IsNullOrEmpty(clientIdSuffix)
                ? options.ClientId
                : $"{options.ClientId}.{clientIdSuffix}",
            MessageTimeoutMs = options.TimeoutProduceMs,
            RequestTimeoutMs = options.TimeoutProduceMs,
        };

        ApplySecurity(options, config);
        return config;
    }

    public static ConsumerConfig BuildConsumerConfig(HellnetKafkaOptions options, string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = options.Brokers,
            GroupId = groupId,
            AutoOffsetReset = options.AutoOffsetReset?.ToLowerInvariant() switch
            {
                "earliest" => Confluent.Kafka.AutoOffsetReset.Earliest,
                "latest" => Confluent.Kafka.AutoOffsetReset.Latest,
                _ => Confluent.Kafka.AutoOffsetReset.Earliest,
            },
            EnableAutoCommit = false,
            ClientId = $"{options.ClientId}.{groupId}",
            GroupProtocol = options.GroupProtocol?.ToLowerInvariant() switch
            {
                "consumer" => Confluent.Kafka.GroupProtocol.Consumer,
                _ => Confluent.Kafka.GroupProtocol.Classic,
            },
            SessionTimeoutMs = options.TimeoutProduceMs,
        };

        ApplySecurity(options, config);
        return config;
    }

    private static void ApplySecurity(HellnetKafkaOptions options, ClientConfig config)
    {
        config.SecurityProtocol = options.SecurityProtocol?.ToLowerInvariant() switch
        {
            "ssl" => Confluent.Kafka.SecurityProtocol.Ssl,
            "sasl_plaintext" => Confluent.Kafka.SecurityProtocol.SaslPlaintext,
            "sasl_ssl" => Confluent.Kafka.SecurityProtocol.SaslSsl,
            _ => Confluent.Kafka.SecurityProtocol.Plaintext,
        };

        if (!string.IsNullOrWhiteSpace(options.SaslMechanism))
        {
            config.SaslMechanism = options.SaslMechanism switch
            {
                "PLAIN" => Confluent.Kafka.SaslMechanism.Plain,
                "SCRAM-SHA-256" => Confluent.Kafka.SaslMechanism.ScramSha256,
                "SCRAM-SHA-512" => Confluent.Kafka.SaslMechanism.ScramSha512,
                "GSSAPI" => Confluent.Kafka.SaslMechanism.Gssapi,
                "OAUTHBEARER" => Confluent.Kafka.SaslMechanism.OAuthBearer,
                _ => Confluent.Kafka.SaslMechanism.Plain,
            };
            config.SaslUsername = options.SaslUsername;
            config.SaslPassword = options.SaslPassword;
        }

        // SSL / TLS
        if (options.SslCaLocation is not null)
            config.SslCaLocation = options.SslCaLocation;

        config.SslEndpointIdentificationAlgorithm =
            string.IsNullOrEmpty(options.SslEndpointIdentificationAlgorithm)
                ? SslEndpointIdentificationAlgorithm.None
                : SslEndpointIdentificationAlgorithm.Https;
    }
}
