using System.Diagnostics.CodeAnalysis;
using System.Text;
using Confluent.Kafka;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.Logging;
using Polly;

namespace Hellnet.Kafka.Internal;

internal sealed class KafkaMessageBus : IMessageBus, IAsyncDisposable
{
    private readonly IProducer<string, byte[]> _producer;
    private readonly IMessageSerializer _serializer;
    private readonly HellnetKafkaOptions _options;
    private readonly ILogger<KafkaMessageBus> _logger;
    private readonly ResiliencePipeline _pipeline;

    public KafkaMessageBus(
        HellnetKafkaOptions options,
        IMessageSerializer serializer,
        ILogger<KafkaMessageBus> logger)
    {
        _options = options;
        _serializer = serializer;
        _logger = logger;
        _pipeline = ResiliencePipelines.Produce(options);

        _producer = new ProducerBuilder<string, byte[]>(
            KafkaConfigBuilder.BuildProducerConfig(options)).Build();
    }

    [ExcludeFromCodeCoverage]
    public async Task PublishAsync<TMessage>(TMessage message, CancellationToken ct = default)
        where TMessage : class, IMessage
    {
        var topic = ResolveTopic(message);
        var data = _serializer.Serialize(message);

        await _pipeline.ExecuteAsync(async cancel =>
        {
            var result = await _producer.ProduceAsync(topic, new Message<string, byte[]>
            {
                Key = message.MessageType,
                Value = data,
                Headers = new Headers
                {
                    new("message.type", Encoding.UTF8.GetBytes(message.MessageType)),
                    new("content.type", Encoding.UTF8.GetBytes(_options.DefaultSerializer)),
                },
            }, cancel);

            _logger.LogDebug(
                "Published {MessageType} to {Topic} [{Partition}] @{Offset}",
                message.MessageType, topic, result.Partition, result.Offset);
        }, ct);
    }

    [ExcludeFromCodeCoverage]
    public async Task PublishBatchAsync<TMessage>(IEnumerable<TMessage> messages, CancellationToken ct = default)
        where TMessage : class, IMessage
    {
        foreach (var message in messages)
        {
            await PublishAsync(message, ct);
        }
    }

    internal string ResolveTopic(IMessage message)
    {
        var prefix = _options.TopicPrefix;
        var topic = string.IsNullOrEmpty(prefix)
            ? message.MessageType
            : $"{prefix}.{message.MessageType}";
        return topic;
    }

    public async ValueTask DisposeAsync()
    {
        _producer?.Flush(TimeSpan.FromSeconds(5));
        _producer?.Dispose();
        await ValueTask.CompletedTask;
    }
}
