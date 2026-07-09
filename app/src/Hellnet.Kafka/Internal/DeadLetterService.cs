using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.Logging;

namespace Hellnet.Kafka.Internal;

/// <summary>
/// Publishes failed messages to a dead-letter topic.
/// DLQ topic = original topic + ".dlq" unless overridden in options.
/// </summary>
internal sealed class DeadLetterService : IAsyncDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly IMessageSerializer _serializer;
    private readonly HellnetKafkaOptions _options;
    private readonly ILogger<DeadLetterService> _logger;

    public DeadLetterService(
        HellnetKafkaOptions options,
        IMessageSerializer serializer,
        ILogger<DeadLetterService> logger)
    {
        _options = options;
        _serializer = serializer;
        _logger = logger;

        var config = new ProducerConfig
        {
            BootstrapServers = options.Brokers,
            Acks = Confluent.Kafka.Acks.Leader,
            ClientId = $"{options.ClientId}.dlq",
        };

        _producer = new ProducerBuilder<string, byte[]>(config).Build();
    }

    [ExcludeFromCodeCoverage]
    public async Task SendToDeadLetterAsync<TMessage>(
        TMessage message,
        IMessageContext context,
        string reason,
        CancellationToken ct = default)
        where TMessage : IMessage
    {
        var dlqTopic = _options.DeadLetterTopic ?? $"{context.Topic}.dlq";
        var data = _serializer.Serialize(message);

        await _producer.ProduceAsync(dlqTopic, new Message<string, byte[]>
        {
            Key = message.MessageType,
            Value = data,
            Headers = new Headers
            {
                new Header("message.type", System.Text.Encoding.UTF8.GetBytes(message.MessageType)),
                new Header("dlq.reason", System.Text.Encoding.UTF8.GetBytes(reason)),
                new Header("dlq.original.topic", System.Text.Encoding.UTF8.GetBytes(context.Topic)),
                new Header("dlq.original.partition", System.Text.Encoding.UTF8.GetBytes(context.Partition.ToString())),
                new Header("dlq.original.offset", System.Text.Encoding.UTF8.GetBytes(context.Offset.ToString())),
            },
        }, ct);

        _logger.LogWarning(
            "Moved {MessageType} [{MessageId}] to DLQ topic {DlqTopic}. Reason: {Reason}",
            message.MessageType, context.MessageId, dlqTopic, reason);
    }

    public async ValueTask DisposeAsync()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        await ValueTask.CompletedTask;
    }
}
