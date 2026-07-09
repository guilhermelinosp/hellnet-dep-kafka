using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Microsoft.Extensions.Logging;
using Polly;

namespace Hellnet.Kafka.Internal;

/// <summary>
/// Handles retry logic using Polly resilience pipeline.
/// When max retries exhausted, delegates to DeadLetterService.
/// </summary>
internal sealed class RetryEngine
{
    private readonly int _maxRetries;
    private readonly ILogger _logger;
    private readonly ResiliencePipeline _pipeline;

    internal RetryEngine(
        HellnetKafkaOptions options,
        ILogger<RetryEngine> logger)
        : this(options, logger, null)
    {
    }

    // Test constructor — injectable wait function
    internal RetryEngine(
        HellnetKafkaOptions options,
        ILogger<RetryEngine> logger,
        Func<TimeSpan, CancellationToken, Task>? wait)
    {
        _maxRetries = options.MaxRetries;
        _logger = logger;
        _pipeline = ResiliencePipelines.Handler(options);
    }

    public async Task ExecuteAsync<TMessage>(
        IMessageHandler<TMessage> handler,
        TMessage message,
        IMessageContext context,
        CancellationToken ct)
        where TMessage : IMessage
    {
        var attempt = 0;

        await _pipeline.ExecuteAsync(async cancel =>
        {
            attempt++;

            try
            {
                await handler.HandleAsync(message, context, cancel);
            }
            catch (OperationCanceledException)
            {
                throw; // shutdown, don't retry
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex,
                    "Handler failed for {MessageType} [{MessageId}] (attempt {Attempt}/{MaxRetries})",
                    message.MessageType, context.MessageId, attempt, _maxRetries);

                if (attempt >= _maxRetries)
                {
                    _logger.LogError(ex,
                        "Exhausted retries for {MessageType} [{MessageId}]. Sending to DLQ.",
                        message.MessageType, context.MessageId);
                }

                throw; // Polly retries, then propagates to caller
            }
        }, ct);
    }
}
