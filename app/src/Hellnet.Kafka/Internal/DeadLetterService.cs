using System.Diagnostics.CodeAnalysis;
using System.Text;
using Confluent.Kafka;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.Logging;
using Polly;

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
    private readonly ResiliencePipeline _pipeline;

    public DeadLetterService(
        HellnetKafkaOptions options,
        IMessageSerializer serializer,
        ILogger<DeadLetterService> logger)
    {
        _options = options;
        _serializer = serializer;
        _logger = logger;
        _pipeline = ResiliencePipelines.DeadLetter(options);

        _producer = new ProducerBuilder<string, byte[]>(
            KafkaConfigBuilder.BuildProducerConfig(options, "dlq")).Build();
    }

    [ExcludeFromCodeCoverage]
    public async Task SendToDeadLetterAsync<TMessage>(
        TMessage message,
        IMessageContext context,
        string reason,
        CancellationToken ct = default)
        where TMessage : class, IMessage
    {
        var dlqTopic = _options.DeadLetterTopic ?? $"{context.Topic}.dlq";
        var data = _serializer.Serialize(message);

        await _pipeline.ExecuteAsync(async cancel =>
        {
            await _producer.ProduceAsync(dlqTopic, new Message<string, byte[]>
            {
                Key = message.MessageType,
                Value = data,
                Headers = new Headers
                {
                    new("message.type", Encoding.UTF8.GetBytes(message.MessageType)),
                    new("dlq.reason", Encoding.UTF8.GetBytes(reason)),
                    new("dlq.original.topic", Encoding.UTF8.GetBytes(context.Topic)),
                    new("dlq.original.partition", Encoding.UTF8.GetBytes(context.Partition.ToString())),
                    new("dlq.original.offset", Encoding.UTF8.GetBytes(context.Offset.ToString())),
                },
            }, cancel);
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
