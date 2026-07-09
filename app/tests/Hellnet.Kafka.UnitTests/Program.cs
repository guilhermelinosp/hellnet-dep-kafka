using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Internal;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Hellnet.Kafka.UnitTests;

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
        foreach (var key in new[]
        {
            "HELLNET_KAFKA_BROKERS",
            "HELLNET_KAFKA_CONSUMER_GROUP",
            "HELLNET_KAFKA_DEFAULT_SERIALIZER",
            "HELLNET_KAFKA_AUTO_REGISTER_HANDLERS",
            "HELLNET_KAFKA_MAX_RETRIES",
            "HELLNET_KAFKA_IDEMPOTENT",
            "HELLNET_KAFKA_ACKS",
            "HELLNET_KAFKA_CLIENT_ID",
            "HELLNET_KAFKA_SASL_MECHANISM",
            "HELLNET_KAFKA_SECURITY_PROTOCOL",
            "HELLNET_KAFKA_SSL_CA_LOCATION",
            "HELLNET_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM",
            "HELLNET_KAFKA_SCHEMA_REGISTRY_URL",
            "HELLNET_KAFKA_TOPIC_PREFIX",
            "HELLNET_KAFKA_GROUP_PROTOCOL",
        })
        {
            Environment.SetEnvironmentVariable(key, null);
        }
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
        Assert.Equal("plaintext", options.SecurityProtocol);
        Assert.Equal("https", options.SslEndpointIdentificationAlgorithm);
        Assert.Null(options.SslCaLocation);
        Assert.Equal("", options.TopicPrefix);
        Assert.Equal("classic", options.GroupProtocol);
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
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_SECURITY_PROTOCOL", "sasl_ssl");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_SSL_CA_LOCATION", "/etc/kafka/ca.crt");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_SCHEMA_REGISTRY_URL", "http://registry:8081");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_TOPIC_PREFIX", "hellnet");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_GROUP_PROTOCOL", "consumer");

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
        Assert.Equal("sasl_ssl", options.SecurityProtocol);
        Assert.Equal("/etc/kafka/ca.crt", options.SslCaLocation);
        Assert.Equal("", options.SslEndpointIdentificationAlgorithm);
        Assert.Equal("http://registry:8081", options.SchemaRegistryUrl);
        Assert.Equal("hellnet", options.TopicPrefix);
        Assert.Equal("consumer", options.GroupProtocol);
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
    public void EnvOrNull_ReturnsFallback_WhenNotSet()
    {
        Assert.Null(KafkaEnvBinder.EnvOrNull("NONEXISTENT", null));
        Assert.Equal("fb", KafkaEnvBinder.EnvOrNull("NONEXISTENT", "fb"));
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
        Assert.Equal("plaintext", opts.SecurityProtocol);
        Assert.Equal("https", opts.SslEndpointIdentificationAlgorithm);
        Assert.Equal("", opts.TopicPrefix);
        Assert.Equal("classic", opts.GroupProtocol);
    }
}

// ============================================================
// HellnetKafkaDefaults tests
// ============================================================

public sealed class HellnetKafkaDefaultsTests
{
    [Fact]
    public void Create_SetsInfraDefaults()
    {
        var opts = HellnetKafkaDefaults.Create();
        Assert.Equal("kafka.hellnet.com.br:9094", opts.Brokers);
        Assert.Equal("sasl_ssl", opts.SecurityProtocol);
        Assert.Equal("SCRAM-SHA-512", opts.SaslMechanism);
        Assert.Equal("hellnet-app", opts.SaslUsername);
        Assert.Equal("hellnet2026", opts.SaslPassword);
        Assert.Equal("", opts.SslEndpointIdentificationAlgorithm);
        Assert.Equal("avro", opts.DefaultSerializer);
        Assert.Equal("https://schema.hellnet.com.br", opts.SchemaRegistryUrl);
        Assert.Equal("hellnet", opts.TopicPrefix);
        Assert.True(opts.Idempotent);
    }
}

// ============================================================
// KafkaEnvBinder with base options tests
// ============================================================

public sealed class KafkaEnvBinderWithBaseTests : IDisposable
{
    public KafkaEnvBinderWithBaseTests() => ClearEnv();
    public void Dispose() => ClearEnv();

    private static void ClearEnv()
    {
        foreach (var key in new[]
        {
            "HELLNET_KAFKA_BROKERS",
            "HELLNET_KAFKA_SASL_MECHANISM",
            "HELLNET_KAFKA_TOPIC_PREFIX",
        })
            Environment.SetEnvironmentVariable(key, null);
    }

    [Fact]
    public void Bind_UsesBaseValues_WhenNoEnvSet()
    {
        var baseOpts = new HellnetKafkaOptions
        {
            Brokers = "base:9092",
            SaslMechanism = "SCRAM-SHA-512",
            TopicPrefix = "baseprefix",
        };

        var result = KafkaEnvBinder.Bind(baseOpts);
        Assert.Equal("base:9092", result.Brokers);
        Assert.Equal("SCRAM-SHA-512", result.SaslMechanism);
        Assert.Equal("baseprefix", result.TopicPrefix);
    }

    [Fact]
    public void Bind_EnvOverridesBase()
    {
        var baseOpts = new HellnetKafkaOptions
        {
            Brokers = "base:9092",
            TopicPrefix = "baseprefix",
        };

        Environment.SetEnvironmentVariable("HELLNET_KAFKA_BROKERS", "env:9092");
        Environment.SetEnvironmentVariable("HELLNET_KAFKA_TOPIC_PREFIX", "envprefix");

        var result = KafkaEnvBinder.Bind(baseOpts);
        Assert.Equal("env:9092", result.Brokers);
        Assert.Equal("envprefix", result.TopicPrefix);
    }

    [Fact]
    public void BindWithDefaults_SetsInfraValues()
    {
        var result = KafkaEnvBinder.Bind(HellnetKafkaDefaults.Create());
        Assert.Equal("kafka.hellnet.com.br:9094", result.Brokers);
        Assert.Equal("sasl_ssl", result.SecurityProtocol);
        Assert.Equal("avro", result.DefaultSerializer);
    }
}

// ============================================================
// KafkaConfigBuilder tests
// ============================================================

public sealed class KafkaConfigBuilderTests
{
    [Fact]
    public void BuildProducerConfig_SetsPlaintext_ByDefault()
    {
        var opts = new HellnetKafkaOptions();
        var config = KafkaConfigBuilder.BuildProducerConfig(opts);
        Assert.Equal(Confluent.Kafka.SecurityProtocol.Plaintext, config.SecurityProtocol);
    }

    [Fact]
    public void BuildProducerConfig_SetsSaslSsl_WhenConfigured()
    {
        var opts = new HellnetKafkaOptions
        {
            SecurityProtocol = "sasl_ssl",
            SaslMechanism = "SCRAM-SHA-512",
            SaslUsername = "user1",
            SaslPassword = "pass1",
            SslCaLocation = "/etc/ca.pem",
            SslEndpointIdentificationAlgorithm = "",
        };

        var config = KafkaConfigBuilder.BuildProducerConfig(opts);
        Assert.Equal(Confluent.Kafka.SecurityProtocol.SaslSsl, config.SecurityProtocol);
        Assert.Equal(Confluent.Kafka.SaslMechanism.ScramSha512, config.SaslMechanism);
        Assert.Equal("user1", config.SaslUsername);
        Assert.Equal("pass1", config.SaslPassword);
        Assert.Equal("/etc/ca.pem", config.SslCaLocation);
        Assert.Equal(Confluent.Kafka.SslEndpointIdentificationAlgorithm.None, config.SslEndpointIdentificationAlgorithm);
    }

    [Fact]
    public void BuildProducerConfig_SetsSsl_WhenConfigured()
    {
        var opts = new HellnetKafkaOptions
        {
            SecurityProtocol = "ssl",
            SslCaLocation = "/etc/ca.pem",
        };
        var config = KafkaConfigBuilder.BuildProducerConfig(opts);
        Assert.Equal(Confluent.Kafka.SecurityProtocol.Ssl, config.SecurityProtocol);
    }

    [Fact]
    public void BuildConsumerConfig_SetsGroupProtocolClassic_ByDefault()
    {
        var opts = new HellnetKafkaOptions();
        var config = KafkaConfigBuilder.BuildConsumerConfig(opts, "my-group");
        Assert.Equal(Confluent.Kafka.GroupProtocol.Classic, config.GroupProtocol);
    }

    [Fact]
    public void BuildConsumerConfig_SetsGroupProtocolConsumer_WhenConfigured()
    {
        var opts = new HellnetKafkaOptions { GroupProtocol = "consumer" };
        var config = KafkaConfigBuilder.BuildConsumerConfig(opts, "my-group");
        Assert.Equal(Confluent.Kafka.GroupProtocol.Consumer, config.GroupProtocol);
    }

    [Fact]
    public void BuildProducerConfig_AppendsClientIdSuffix()
    {
        var opts = new HellnetKafkaOptions { ClientId = "my-app" };
        var config = KafkaConfigBuilder.BuildProducerConfig(opts, "dlq");
        Assert.Equal("my-app.dlq", config.ClientId);
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

    [Fact]
    public void AddHellnetKafka_RegistersJsonSerializer_ByDefault()
    {
        var services = new ServiceCollection();
        services.AddHellnetKafka(new HellnetKafkaOptions { AutoRegisterHandlers = false });
        var sp = services.BuildServiceProvider();
        var serializer = sp.GetRequiredService<IMessageSerializer>();
        Assert.IsType<JsonMessageSerializer>(serializer);
    }

    [Fact]
    public void AddHellnetKafka_SetsInfraBrokers()
    {
        var services = new ServiceCollection();
        services.AddHellnetKafka();
        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<HellnetKafkaOptions>();
        Assert.Equal("kafka.hellnet.com.br:9094", opts.Brokers);
        Assert.Equal("sasl_ssl", opts.SecurityProtocol);
        Assert.Equal("avro", opts.DefaultSerializer);
        Assert.Equal("hellnet", opts.TopicPrefix);
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
    public void ResolveTopic_ReturnsMessageType_WhenNoPrefix()
    {
        var options = new HellnetKafkaOptions { TopicPrefix = "" };
        var logger = NullLogger<KafkaMessageBus>.Instance;
        var serializer = new JsonMessageSerializer();
        var bus = new KafkaMessageBus(options, serializer, logger);

        var msg = new TestMessage();
        var topic = bus.ResolveTopic(msg);
        Assert.Equal("test.message.v1", topic);

        bus.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [Fact]
    public void ResolveTopic_PrependsPrefix_WhenConfigured()
    {
        var options = new HellnetKafkaOptions { TopicPrefix = "hellnet" };
        var logger = NullLogger<KafkaMessageBus>.Instance;
        var serializer = new JsonMessageSerializer();
        var bus = new KafkaMessageBus(options, serializer, logger);

        var msg = new TestMessage();
        var topic = bus.ResolveTopic(msg);
        Assert.Equal("hellnet.test.message.v1", topic);

        bus.DisposeAsync().AsTask().GetAwaiter().GetResult();
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
        var handlers = KafkaConsumerHost.DiscoverHandlers();
        Assert.Contains(typeof(TestHandler), handlers);
    }

    [Fact]
    public void DiscoverHandlers_ReturnsHandlers_RegardlessOfOption()
    {
        var handlers = KafkaConsumerHost.DiscoverHandlers();
        Assert.Contains(typeof(TestHandler), handlers);
    }

    [Fact]
    public void ResolveTopicFromMessageType_UsesMessageTypeProperty()
    {
        var topic = KafkaConsumerHost.ResolveTopicFromMessageType(typeof(TestMessage));
        Assert.Equal("test.message.v1", topic);
    }
}

// ============================================================
// ResiliencePipelines tests
// ============================================================

public sealed class ResiliencePipelinesTests
{
    [Fact]
    public async Task ProducePipeline_ExecutesSuccessfully()
    {
        var opts = new HellnetKafkaOptions();
        var pipeline = ResiliencePipelines.Produce(opts);
        var result = await pipeline.ExecuteAsync(async _ => { await Task.CompletedTask; return "ok"; }, CancellationToken.None);
        Assert.Equal("ok", result);
    }

    [Fact]
    public async Task HandlerPipeline_ExecutesSuccessfully()
    {
        var opts = new HellnetKafkaOptions();
        var pipeline = ResiliencePipelines.Handler(opts);
        await pipeline.ExecuteAsync(_ => { return ValueTask.CompletedTask; }, CancellationToken.None);
    }

    [Fact]
    public async Task DeadLetterPipeline_ExecutesSuccessfully()
    {
        var opts = new HellnetKafkaOptions();
        var pipeline = ResiliencePipelines.DeadLetter(opts);
        await pipeline.ExecuteAsync(_ => { return ValueTask.CompletedTask; }, CancellationToken.None);
    }

    [Fact]
    public async Task SchemaRegistryPipeline_ExecutesSuccessfully()
    {
        var opts = new HellnetKafkaOptions();
        var pipeline = ResiliencePipelines.SchemaRegistry(opts);
        await pipeline.ExecuteAsync(_ => { return ValueTask.CompletedTask; }, CancellationToken.None);
    }
}

// ============================================================
// HellnetKafkaOptions resilience defaults tests
// ============================================================

public sealed class HellnetKafkaResilienceOptionsTests
{
    [Fact]
    public void ResilienceDefaults_AreSane()
    {
        var opts = new HellnetKafkaOptions();
        Assert.Equal(3, opts.MaxRetries);
        Assert.Equal(200, opts.RetryDelayMs);
        Assert.Equal(30_000, opts.RetryMaxDelayMs);
        Assert.Equal(100, opts.RetryJitterMs);
        Assert.Equal(30_000, opts.TimeoutProduceMs);
        Assert.Equal(10_000, opts.TimeoutSchemaRegistryMs);
        Assert.Equal(5, opts.CircuitBreakerFailureCount);
        Assert.Equal(30_000, opts.CircuitBreakerDurationMs);
    }
}

// ============================================================
// KafkaConfigBuilder timeout tests
// ============================================================

public sealed class KafkaConfigBuilderTimeoutTests
{
    [Fact]
    public void ProducerConfig_SetsTimeouts()
    {
        var opts = new HellnetKafkaOptions { TimeoutProduceMs = 15000 };
        var config = KafkaConfigBuilder.BuildProducerConfig(opts);
        Assert.Equal(15000, config.MessageTimeoutMs);
        Assert.Equal(15000, config.RequestTimeoutMs);
    }

    [Fact]
    public void ConsumerConfig_SetsTimeouts()
    {
        var opts = new HellnetKafkaOptions { TimeoutProduceMs = 20000 };
        var config = KafkaConfigBuilder.BuildConsumerConfig(opts, "test-group");
        Assert.Equal(20000, config.SessionTimeoutMs);
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
