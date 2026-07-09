using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Timeout;
using Polly;

namespace Hellnet.Kafka.Configuration;

/// <summary>
/// Builds Polly resilience pipelines from HellnetKafkaOptions.
/// Each pipeline composes: Timeout → Retry → CircuitBreaker.
/// </summary>
internal static class ResiliencePipelines
{
    /// <summary>Pipeline for produce operations (IMessageBus).</summary>
    public static ResiliencePipeline Produce(HellnetKafkaOptions o) => Builder(o)
        .AddRetry(o)
        .AddCircuitBreaker(o)
        .Build();

    /// <summary>Pipeline for DLQ produce operations.</summary>
    public static ResiliencePipeline DeadLetter(HellnetKafkaOptions o) => Builder(o)
        .AddRetry(o)
        .Build();

    /// <summary>Pipeline for handler execution (RetryEngine).</summary>
    public static ResiliencePipeline Handler(HellnetKafkaOptions o) => Builder(o)
        .AddRetry(o, Math.Max(0, o.MaxRetries - 1)) // MaxRetries = total attempts, Polly = retries
        .Build();

    /// <summary>Pipeline for Schema Registry calls.</summary>
    public static ResiliencePipeline SchemaRegistry(HellnetKafkaOptions o)
    {
        var timeout = new TimeoutStrategyOptions
        {
            Timeout = TimeSpan.FromMilliseconds(o.TimeoutSchemaRegistryMs),
            OnTimeout = args =>
            {
                Console.Error.WriteLine($"[Hellnet.Kafka] Schema Registry timeout after {o.TimeoutSchemaRegistryMs}ms");
                return default;
            },
        };

        var retry = new RetryStrategyOptions
        {
            MaxRetryAttempts = 2,
            Delay = TimeSpan.FromMilliseconds(500),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
        };

        return new ResiliencePipelineBuilder()
            .AddTimeout(timeout)
            .AddRetry(retry)
            .Build();
    }

    private static ResiliencePipelineBuilder Builder(HellnetKafkaOptions o)
    {
        var timeout = new TimeoutStrategyOptions
        {
            Timeout = TimeSpan.FromMilliseconds(o.TimeoutProduceMs),
            OnTimeout = args =>
            {
                Console.Error.WriteLine($"[Hellnet.Kafka] Timeout after {o.TimeoutProduceMs}ms");
                return default;
            },
        };

        return new ResiliencePipelineBuilder()
            .AddTimeout(timeout);
    }

    private static ResiliencePipelineBuilder AddRetry(
        this ResiliencePipelineBuilder builder, HellnetKafkaOptions o, int? maxRetries = null)
    {
        var retry = new RetryStrategyOptions
        {
            MaxRetryAttempts = maxRetries ?? 3,
            Delay = TimeSpan.FromMilliseconds(o.RetryDelayMs),
            MaxDelay = TimeSpan.FromMilliseconds(o.RetryMaxDelayMs),
            BackoffType = DelayBackoffType.Exponential,
            UseJitter = true,
        };

        return builder.AddRetry(retry);
    }

    private static ResiliencePipelineBuilder AddCircuitBreaker(
        this ResiliencePipelineBuilder builder, HellnetKafkaOptions o)
    {
        var cb = new CircuitBreakerStrategyOptions
        {
            FailureRatio = 1.0,
            MinimumThroughput = o.CircuitBreakerFailureCount,
            SamplingDuration = TimeSpan.FromMilliseconds(o.CircuitBreakerDurationMs),
            BreakDuration = TimeSpan.FromMilliseconds(o.CircuitBreakerDurationMs),
        };

        return builder.AddCircuitBreaker(cb);
    }
}
