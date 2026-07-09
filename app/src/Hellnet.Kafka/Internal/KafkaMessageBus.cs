using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.Logging;

namespace Hellnet.Kafka.Internal;

internal sealed class KafkaMessageBus : IMessageBus, IAsyncDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly IMessageSerializer _serializer;
    private readonly HellnetKafkaOptions _options;
    private readonly ILogger<KafkaMessageBus> _logger;

    public KafkaMessageBus(
        HellnetKafkaOptions options,
        IMessageSerializer serializer,
        ILogger<KafkaMessageBus> logger)
    {
        _options = options;
        _serializer = serializer;
        _logger = logger;

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
            ClientId = options.ClientId,
        };

        if (!string.IsNullOrWhiteSpace(options.SaslMechanism))
        {
            config.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            config.SaslMechanism = options.SaslMechanism switch
            {
                "PLAIN" => Confluent.Kafka.SaslMechanism.Plain,
                "SCRAM-SHA-256" => Confluent.Kafka.SaslMechanism.ScramSha256,
                "SCRAM-SHA-512" => Confluent.Kafka.SaslMechanism.ScramSha512,
                "GSSAPI" => Confluent.Kafka.SaslMechanism.Gssapi,
                "OAUTHBEARER" => Confluent.Kafka.SaslMechanism.OAuthBearer,
                _ => null,
            };
            config.SaslUsername = options.SaslUsername;
            config.SaslPassword = options.SaslPassword;
        }

        _producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    [ExcludeFromCodeCoverage]
    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken ct = default)
        where TMessage : IMessage
    {
        var topic = ResolveTopic(message);
        var data = _serializer.Serialize(message);

        var result = await _producer.ProduceAsync(topic, new Message<string, byte[]>
        {
            Key = message.MessageType,
            Value = data,
            Headers = new Headers
            {
                new Header("message.type", System.Text.Encoding.UTF8.GetBytes(message.MessageType)),
                new Header("content.type", System.Text.Encoding.UTF8.GetBytes("json")),
            },
        }, ct);

        _logger.LogDebug(
            "Published {MessageType} to {Topic} [{Partition}] @{Offset}",
            message.MessageType, topic, result.Partition, result.Offset);
    }

    [ExcludeFromCodeCoverage]
    public async Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken ct = default)
        where TMessage : IMessage
    {
        foreach (var message in messages)
        {
            await PublishAsync(message, ct);
        }
    }

    internal static string ResolveTopic(IMessage message)
    {
        // Future: allow topic mapping via configuration
        return message.MessageType;
    }

    public async ValueTask DisposeAsync()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        await ValueTask.CompletedTask;
    }
}
