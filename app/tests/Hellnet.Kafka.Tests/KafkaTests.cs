using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Internal;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Hellnet.Kafka.Tests;

// ============================================================
// Test doubles
// ============================================================

public sealed record TestMessage : IMessage
{
    public string MessageType => "test.message.v1";
    public string Data { get; init; } = string.Empty;
}

[MessageHandler(Topic = "test.message.v1", ConsumerGroup = "test-group", MaxRetries = 2)]
public sealed class TestHandler : IMessageHandler<TestMessage>
{
    public int InvocationCount { get; private set; }

    public static readonly List<IMessageContext> ReceivedContexts = [];

    public Task HandleAsync(TestMessage message, IMessageContext context, CancellationToken ct)
    {
        InvocationCount++;
        ReceivedContexts.Add(context);
        return Task.CompletedTask;
    }
}

public sealed class FailingHandler : IMessageHandler<TestMessage>
{
    private readonly int _failUntil;

    public FailingHandler(int failUntil = int.MaxValue)
    {
        _failUntil = failUntil;
    }

    public int InvocationCount { get; private set; }

    public Task HandleAsync(TestMessage message, IMessageContext context, CancellationToken ct)
    {
        InvocationCount++;
        if (InvocationCount <= _failUntil)
            throw new InvalidOperationException($"Simulated failure #{InvocationCount}");
        return Task.CompletedTask;
    }
}

// ============================================================
// KafkaEnvBinder tests
// ============================================================

public sealed class KafkaEnvBinderTests : IDisposable
{
    public KafkaEnvBinderTests()
    {
        ClearEnv();
    }

    public void Dispose()
    {
        ClearEnv();
    }

    private static void ClearEnv()
    {
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_BROKERS", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_CONSUMER_GROUP", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_DEFAULT_SERIALIZER", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_AUTO_REGISTER_HANDLERS", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_MAX_RETRIES", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_IDEMPOTENT", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_ACKS", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_CLIENT_ID", null);
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_SASL_MECHANISM", null);
    }

    [Fact]
    public void Bind_UsesDefaults_WhenNoEnvSet()
    {
        var options = KafkaEnvBinder.Bind();
        Assert.Equal("localhost:9092", options.Brokers);
        Assert.Equal("json", options.DefaultSerializer);
        Assert.True(options.AutoRegisterHandlers);
        Assert.Equal(3, options.MaxRetries);
        Assert.True(options.Idempotent);
        Assert.Equal("all", options.Acks);
        Assert.Equal("earliest", options.AutoOffsetReset);
        Assert.Equal(Environment.MachineName, options.ClientId);
    }

    [Fact]
    public void Bind_ReadsFromEnv()
    {
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_CONSUMER_GROUP", "my-service");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_DEFAULT_SERIALIZER", "avro");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_AUTO_REGISTER_HANDLERS", "false");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_MAX_RETRIES", "5");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_IDEMPOTENT", "false");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_ACKS", "leader");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_CLIENT_ID", "my-client");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_SASL_MECHANISM", "PLAIN");

        var options = KafkaEnvBinder.Bind();
        Assert.Equal("kafka-1:9092,kafka-2:9092", options.Brokers);
        Assert.Equal("my-service", options.ConsumerGroup);
        Assert.Equal("avro", options.DefaultSerializer);
        Assert.False(options.AutoRegisterHandlers);
        Assert.Equal(5, options.MaxRetries);
        Assert.False(options.Idempotent);
        Assert.Equal("leader", options.Acks);
        Assert.Equal("my-client", options.ClientId);
        Assert.Equal("PLAIN", options.SaslMechanism);
    }

    [Fact]
    public void EnvBool_ReturnsDefault_WhenNotSet()
    {
        Assert.True(KafkaEnvBinder.EnvBool("NONEXISTENT", true));
        Assert.False(KafkaEnvBinder.EnvBool("NONEXISTENT", false));
    }

    [Fact]
    public void EnvBool_ParsesCorrectly()
    {
        Environment.SetEnvironmentVariable("TEST_BOOL", "true");
        Assert.True(KafkaEnvBinder.EnvBool("TEST_BOOL", false));
        Environment.SetEnvironmentVariable("TEST_BOOL", "false");
        Assert.False(KafkaEnvBinder.EnvBool("TEST_BOOL", true));
    }

    [Fact]
    public void EnvInt_ReturnsDefault_WhenNotSet()
    {
        Assert.Equal(42, KafkaEnvBinder.EnvInt("NONEXISTENT", 42));
    }

    [Fact]
    public void EnvInt_ParsesCorrectly()
    {
        Environment.SetEnvironmentVariable("TEST_INT", "99");
        Assert.Equal(99, KafkaEnvBinder.EnvInt("TEST_INT", 0));
    }

    [Fact]
    public void Env_ReturnsFallback_WhenNotSet()
    {
        Assert.Equal("fallback", KafkaEnvBinder.Env("NONEXISTENT", "fallback"));
    }

    [Fact]
    public void EnvOrNull_ReturnsNull_WhenNotSet()
    {
        Assert.Null(KafkaEnvBinder.EnvOrNull("NONEXISTENT"));
    }
}

// ============================================================
// HellnetKafkaOptions tests
// ============================================================

public sealed class HellnetKafkaOptionsTests
{
    [Fact]
    public void Defaults_AreSane()
    {
        var opts = new HellnetKafkaOptions();
        Assert.Equal("localhost:9092", opts.Brokers);
        Assert.Equal("json", opts.DefaultSerializer);
        Assert.True(opts.AutoRegisterHandlers);
        Assert.True(opts.Idempotent);
        Assert.Equal("earliest", opts.AutoOffsetReset);
        Assert.NotNull(opts.ClientId);
    }
}

// ============================================================
// JsonMessageSerializer tests
// ============================================================

public sealed class JsonMessageSerializerTests
{
    private readonly JsonMessageSerializer _serializer = new();

    [Fact]
    public void SerializeAndDeserialize_Roundtrip()
    {
        var message = new TestMessage { Data = "hello-kafka" };
        var bytes = _serializer.Serialize(message);
        Assert.NotNull(bytes);
        Assert.True(bytes.Length > 0);

        var deserialized = _serializer.Deserialize<TestMessage>(bytes);
        Assert.NotNull(deserialized);
        Assert.Equal("hello-kafka", deserialized.Data);
        Assert.Equal("test.message.v1", deserialized.MessageType);
    }

    [Fact]
    public void Serialize_ProducesSnakeCaseJson()
    {
        var message = new TestMessage { Data = "test" };
        var bytes = _serializer.Serialize(message);
        var json = System.Text.Encoding.UTF8.GetString(bytes);
        Assert.Contains("message_type", json); // snake_case
        Assert.Contains("test.message.v1", json);
    }
}

// ============================================================
// MessageContext tests
// ============================================================

public sealed class MessageContextTests
{
    [Fact]
    public void CreatesFromConsumeResult()
    {
        var headers = new Confluent.Kafka.Headers
        {
            new("delivery.attempt", System.Text.Encoding.UTF8.GetBytes("2")),
            new("custom-key", System.Text.Encoding.UTF8.GetBytes("custom-value")),
        };

        var ctx = new MessageContext(
            "msg-123",
            "test.topic",
            DateTime.UtcNow,
            new Confluent.Kafka.Partition(3),
            new Confluent.Kafka.Offset(42),
            headers);

        Assert.Equal("msg-123", ctx.MessageId);
        Assert.Equal("test.topic", ctx.Topic);
        Assert.Equal(3, ctx.Partition);
        Assert.Equal(42L, ctx.Offset);
        Assert.Equal(2, ctx.DeliveryAttempt);
        Assert.Equal("custom-value", ctx.Headers["custom-key"]);
    }

    [Fact]
    public void HandlesNullHeaders()
    {
        var ctx = new MessageContext("id", "topic", DateTime.UtcNow,
            new Confluent.Kafka.Partition(0), new Confluent.Kafka.Offset(0), null);

        Assert.Empty(ctx.Headers);
        Assert.Equal(0, ctx.DeliveryAttempt);
    }

    [Fact]
    public void DeliveryAttempt_ReturnsZero_WhenHeaderMissing()
    {
        var headers = new Confluent.Kafka.Headers();
        var ctx = new MessageContext("id", "topic", DateTime.UtcNow,
            new Confluent.Kafka.Partition(0), new Confluent.Kafka.Offset(0), headers);

        Assert.Equal(0, ctx.DeliveryAttempt);
    }
}

// ============================================================
// RetryEngine tests
// ============================================================

public sealed class RetryEngineTests
{
    private readonly ILogger<RetryEngine> _logger = NullLogger<RetryEngine>.Instance;

    [Fact]
    public async Task ExecuteAsync_Succeeds_OnFirstAttempt()
    {
        var options = new HellnetKafkaOptions { MaxRetries = 3 };
        var engine = new RetryEngine(options, _logger, (_, _) => Task.CompletedTask);
        var handler = new TestHandler();
        var ctx = CreateContext();
        var message = new TestMessage();

        await engine.ExecuteAsync(handler, message, ctx, CancellationToken.None);

        Assert.Equal(1, handler.InvocationCount);
    }

    [Fact]
    public async Task ExecuteAsync_Retries_ThenSucceeds()
    {
        var options = new HellnetKafkaOptions { MaxRetries = 5 };
        var engine = new RetryEngine(options, _logger, (_, _) => Task.CompletedTask);
        var handler = new FailingHandler(failUntil: 2); // fail twice, succeed on 3rd
        var ctx = CreateContext();
        var message = new TestMessage();

        await engine.ExecuteAsync(handler, message, ctx, CancellationToken.None);

        Assert.Equal(3, handler.InvocationCount);
    }

    [Fact]
    public async Task ExecuteAsync_Throws_WhenRetriesExhausted()
    {
        var options = new HellnetKafkaOptions { MaxRetries = 2 };
        var engine = new RetryEngine(options, _logger, (_, _) => Task.CompletedTask);
        var handler = new FailingHandler(failUntil: 99); // always fails
        var ctx = CreateContext();
        var message = new TestMessage();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => engine.ExecuteAsync(handler, message, ctx, CancellationToken.None));

        Assert.Contains("Simulated failure", ex.Message);
        Assert.Equal(2, handler.InvocationCount); // maxRetries = 2 attempts
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsOperationCanceled_WithoutRetry()
    {
        var options = new HellnetKafkaOptions { MaxRetries = 3 };
        var engine = new RetryEngine(options, _logger, (_, _) => Task.CompletedTask);

        // Handler that throws cancellation
        var handler = new FailingHandler(failUntil: 99);
        // Replace handler behavior to throw cancellation
        var ctx = CreateContext();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            var handler2 = new ThrowingCancellationHandler();
            await engine.ExecuteAsync(handler2, new TestMessage(), ctx, CancellationToken.None);
        });
    }

    private static IMessageContext CreateContext()
    {
        return new MessageContext("test-id", "test.topic", DateTime.UtcNow,
            new Confluent.Kafka.Partition(0), new Confluent.Kafka.Offset(0), null);
    }

    private sealed class ThrowingCancellationHandler : IMessageHandler<TestMessage>
    {
        public Task HandleAsync(TestMessage message, IMessageContext context, CancellationToken ct)
            => throw new OperationCanceledException();
    }
}

// ============================================================
// DependencyInjection tests
// ============================================================

public sealed class DependencyInjectionTests
{
    [Fact]
    public void AddHellnetKafka_RegistersCoreServices()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHellnetKafka();

        var sp = services.BuildServiceProvider();

        Assert.NotNull(sp.GetRequiredService<HellnetKafkaOptions>());
        Assert.NotNull(sp.GetRequiredService<IMessageSerializer>());
        Assert.NotNull(sp.GetRequiredService<IMessageBus>());
        Assert.NotNull(sp.GetRequiredService<RetryEngine>());
        Assert.NotNull(sp.GetRequiredService<DeadLetterService>());
    }

    [Fact]
    public void AddHellnetKafka_WithOptions_RegistersCorrectly()
    {
        var options = new HellnetKafkaOptions
        {
            Brokers = "test:9092",
            AutoRegisterHandlers = false,
        };

        var services = new ServiceCollection();
        services.AddHellnetKafka(options);

        var sp = services.BuildServiceProvider();

        var resolved = sp.GetRequiredService<HellnetKafkaOptions>();
        Assert.Equal("test:9092", resolved.Brokers);
    }

    [Fact]
    public void AddHellnetKafka_ReturnsServiceCollection()
    {
        var services = new ServiceCollection();
        var result = services.AddHellnetKafka();
        Assert.Same(services, result);
    }

    [Fact]
    public void AddHellnetKafka_WithOptions_ReturnsServiceCollection()
    {
        var services = new ServiceCollection();
        var result = services.AddHellnetKafka(new HellnetKafkaOptions());
        Assert.Same(services, result);
    }

    [Fact]
    public void AddHellnetKafka_RegistersHandler_WhenDiscovered()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHellnetKafka(new HellnetKafkaOptions
        {
            AutoRegisterHandlers = true,
            Brokers = "localhost:9092",
        });

        var sp = services.BuildServiceProvider();
        var handler = sp.GetService<IMessageHandler<TestMessage>>();
        Assert.NotNull(handler);
    }

    [Fact]
    public void AddHellnetKafka_DoesNotRegisterHandler_WhenAutoDisabled()
    {
        var services = new ServiceCollection();
        services.AddHellnetKafka(new HellnetKafkaOptions
        {
            AutoRegisterHandlers = false,
        });

        var sp = services.BuildServiceProvider();
        var handler = sp.GetService<IMessageHandler<TestMessage>>();
        Assert.Null(handler);
    }
}

// ============================================================
// KafkaMessageBus tests
// ============================================================

public sealed class KafkaMessageBusTests
{
    [Fact]
    public async Task Constructor_DoesNotThrow()
    {
        var options = new HellnetKafkaOptions { Brokers = "localhost:9092" };
        var logger = NullLogger<KafkaMessageBus>.Instance;
        var serializer = new JsonMessageSerializer();

        await using var bus = new KafkaMessageBus(options, serializer, logger);
        Assert.NotNull(bus);
    }

    [Fact]
    public void ResolveTopic_ReturnsMessageType()
    {
        var msg = new TestMessage();
        Assert.Equal("test.message.v1", KafkaMessageBus.ResolveTopic(msg));
    }
}

// ============================================================
// DeadLetterService tests
// ============================================================

public sealed class DeadLetterServiceTests
{
    [Fact]
    public async Task Constructor_DoesNotThrow()
    {
        var options = new HellnetKafkaOptions { Brokers = "localhost:9092" };
        var logger = NullLogger<DeadLetterService>.Instance;
        var serializer = new JsonMessageSerializer();

        await using var service = new DeadLetterService(options, serializer, logger);
        Assert.NotNull(service);
    }
}

// ============================================================
// KafkaConsumerHost tests
// ============================================================

public sealed class KafkaConsumerHostTests
{
    [Fact]
    public void Constructor_DoesNotThrow()
    {
        var options = new HellnetKafkaOptions { Brokers = "localhost:9092" };
        var services = new ServiceCollection();
        services.AddLogging();
        var sp = services.BuildServiceProvider();
        var logger = NullLogger<KafkaConsumerHost>.Instance;

        var host = new KafkaConsumerHost(options, sp, logger);
        Assert.NotNull(host);
    }

    [Fact]
    public void DiscoverHandlers_FindsTestHandler()
    {
        var options = new HellnetKafkaOptions { AutoRegisterHandlers = true };
        var services = new ServiceCollection();
        services.AddLogging();
        var sp = services.BuildServiceProvider();
        var logger = NullLogger<KafkaConsumerHost>.Instance;

        var host = new KafkaConsumerHost(options, sp, logger);
        var handlers = host.DiscoverHandlers();
        Assert.Contains(typeof(TestHandler), handlers);
    }

    [Fact]
    public void DiscoverHandlers_ReturnsEmpty_WhenAutoDisabled()
    {
        var options = new HellnetKafkaOptions { AutoRegisterHandlers = false };
        var services = new ServiceCollection();
        services.AddLogging();
        var sp = services.BuildServiceProvider();
        var logger = NullLogger<KafkaConsumerHost>.Instance;

        var host = new KafkaConsumerHost(options, sp, logger);
        var handlers = host.DiscoverHandlers();
        Assert.Empty(handlers);
    }

    [Fact]
    public void ResolveTopicFromMessageType_UsesMessageTypeProperty()
    {
        var topic = KafkaConsumerHost.ResolveTopicFromMessageType(typeof(TestMessage));
        Assert.Equal("test.message.v1", topic);
    }
}

// ============================================================
// Integration-style: RetryEngine via DI
// ============================================================

public sealed class RetryEngineDiTests
{
    [Fact]
    public void RetryEngine_ResolvedViaDi_UsesFactory()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddHellnetKafka(new HellnetKafkaOptions
        {
            Brokers = "localhost:9092",
            MaxRetries = 2,
            AutoRegisterHandlers = false,
        });

        var sp = services.BuildServiceProvider();
        var engine = sp.GetRequiredService<RetryEngine>();
        Assert.NotNull(engine);
    }
}
