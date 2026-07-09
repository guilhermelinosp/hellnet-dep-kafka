using Hellnet.Kafka.Abstractions;
using Microsoft.Extensions.Logging;

namespace Hellnet.Kafka.Internal;

/// <summary>
/// Handles retry logic with exponential backoff.
/// When max retries exhausted, delegates to DeadLetterService.
/// </summary>
internal sealed class RetryEngine
{
    private readonly int _maxRetries;
    private readonly ILogger _logger;
    private readonly Func<TimeSpan, CancellationToken, Task> _wait;

    // DI constructor — resolve all parameters from container
    internal RetryEngine(
        Configuration.HellnetKafkaOptions options,
        ILogger<RetryEngine> logger)
        : this(options, logger, null)
    {
    }

    // Test constructor — injectable wait function
    internal RetryEngine(
        Configuration.HellnetKafkaOptions options,
        ILogger<RetryEngine> logger,
        Func<TimeSpan, CancellationToken, Task>? wait)
    {
        _maxRetries = options.MaxRetries;
        _logger = logger;
        _wait = wait ?? ((delay, ct) => Task.Delay(delay, ct));
    }

    public async Task ExecuteAsync<TMessage>(
        IMessageHandler<TMessage> handler,
        TMessage message,
        IMessageContext context,
        CancellationToken ct)
        where TMessage : IMessage
    {
        var attempt = 0;
        while (true)
        {
            attempt++;

            try
            {
                await handler.HandleAsync(message, context, ct);
                return; // success
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
                    throw; // caller sends to DLQ
                }

                var delay = TimeSpan.FromMilliseconds(Math.Pow(2, attempt) * 100);
                await _wait(delay, ct);
            }
        }
    }
}
