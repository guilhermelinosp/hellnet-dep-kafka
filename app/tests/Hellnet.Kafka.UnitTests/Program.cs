using AutoFixture;
using FluentAssertions;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Internal;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
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
        options.Brokers.Should().Be("localhost:9092");
        options.DefaultSerializer.Should().Be("json");
        options.AutoRegisterHandlers.Should().BeTrue();
        options.MaxRetries.Should().Be(3);
        options.Idempotent.Should().BeTrue();
        options.Acks.Should().Be("all");
        options.AutoOffsetReset.Should().Be("earliest");
        options.ClientId.Should().Be(Environment.MachineName);
        options.SecurityProtocol.Should().Be("plaintext");
        options.SslEndpointIdentificationAlgorithm.Should().Be("https");
        options.SslCaLocation.Should().BeNull();
        options.TopicPrefix.Should().Be("");
        options.GroupProtocol.Should().Be("classic");
    }

    [Fact]
    public void Bind_ReadsFromEnv()
    {
        var fixture = new Fixture();
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
        options.Brokers.Should().Be("kafka-1:9092,kafka-2:9092");
        options.ConsumerGroup.Should().Be("my-service");
        options.DefaultSerializer.Should().Be("avro");
        options.AutoRegisterHandlers.Should().BeFalse();
        options.MaxRetries.Should().Be(5);
        options.Idempotent.Should().BeFalse();
        options.Acks.Should().Be("leader");
        options.ClientId.Should().Be("my-client");
        options.SaslMechanism.Should().Be("PLAIN");
        options.SecurityProtocol.Should().Be("sasl_ssl");
        options.SslCaLocation.Should().Be("/etc/kafka/ca.crt");
        options.SslEndpointIdentificationAlgorithm.Should().Be("");
        options.SchemaRegistryUrl.Should().Be("http://registry:8081");
        options.TopicPrefix.Should().Be("hellnet");
        options.GroupProtocol.Should().Be("consumer");
    }

    [Fact]
    public void EnvBool_ReturnsDefault_WhenNotSet()
    {
        KafkaEnvBinder.EnvBool("NONEXISTENT", true).Should().BeTrue();
        KafkaEnvBinder.EnvBool("NONEXISTENT", false).Should().BeFalse();
    }

    [Fact]
    public void EnvBool_ParsesCorrectly()
    {
        Environment.SetEnvironmentVariable("TEST_BOOL", "true");
        KafkaEnvBinder.EnvBool("TEST_BOOL", false).Should().BeTrue();
        Environment.SetEnvironmentVariable("TEST_BOOL", "false");
        KafkaEnvBinder.EnvBool("TEST_BOOL", true).Should().BeFalse();
    }

    [Fact]
    public void EnvInt_ReturnsDefault_WhenNotSet()
    {
        KafkaEnvBinder.EnvInt("NONEXISTENT", 42).Should().Be(42);
    }

    [Fact]
    public void EnvInt_ParsesCorrectly()
    {
        Environment.SetEnvironmentVariable("TEST_INT", "99");
        KafkaEnvBinder.EnvInt("TEST_INT", 0).Should().Be(99);
    }

    [Fact]
    public void Env_ReturnsFallback_WhenNotSet()
    {
        KafkaEnvBinder.Env("NONEXISTENT", "fallback").Should().Be("fallback");
    }

    [Fact]
    public void EnvOrNull_ReturnsFallback_WhenNotSet()
    {
        KafkaEnvBinder.EnvOrNull("NONEXISTENT", null).Should().BeNull();
        KafkaEnvBinder.EnvOrNull("NONEXISTENT", "fb").Should().Be("fb");
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
        opts.Brokers.Should().Be("localhost:9092");
        opts.DefaultSerializer.Should().Be("json");
        opts.AutoRegisterHandlers.Should().BeTrue();
        opts.Idempotent.Should().BeTrue();
        opts.AutoOffsetReset.Should().Be("earliest");
        opts.ClientId.Should().NotBeNullOrEmpty();
        opts.SecurityProtocol.Should().Be("plaintext");
        opts.SslEndpointIdentificationAlgorithm.Should().Be("https");
        opts.TopicPrefix.Should().Be("");
        opts.GroupProtocol.Should().Be("classic");
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
        opts.Brokers.Should().Be("kafka.hellnet.com.br:9094");
        opts.SecurityProtocol.Should().Be("sasl_ssl");
        opts.SaslMechanism.Should().Be("SCRAM-SHA-512");
        opts.SaslUsername.Should().Be("hellnet-app");
        opts.SaslPassword.Should().Be("hellnet2026");
        opts.SslEndpointIdentificationAlgorithm.Should().Be("");
        opts.DefaultSerializer.Should().Be("avro");
        opts.SchemaRegistryUrl.Should().Be("https://schema.hellnet.com.br");
        opts.TopicPrefix.Should().Be("hellnet");
        opts.Idempotent.Should().BeTrue();
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
        result.Brokers.Should().Be("base:9092");
        result.SaslMechanism.Should().Be("SCRAM-SHA-512");
        result.TopicPrefix.Should().Be("baseprefix");
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
        result.Brokers.Should().Be("env:9092");
        result.TopicPrefix.Should().Be("envprefix");
    }

    [Fact]
    public void BindWithDefaults_SetsInfraValues()
    {
        var result = KafkaEnvBinder.Bind(HellnetKafkaDefaults.Create());
        result.Brokers.Should().Be("kafka.hellnet.com.br:9094");
        result.SecurityProtocol.Should().Be("sasl_ssl");
        result.DefaultSerializer.Should().Be("avro");
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
        config.SecurityProtocol.Should().Be(Confluent.Kafka.SecurityProtocol.Plaintext);
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
        config.SecurityProtocol.Should().Be(Confluent.Kafka.SecurityProtocol.SaslSsl);
        config.SaslMechanism.Should().Be(Confluent.Kafka.SaslMechanism.ScramSha512);
        config.SaslUsername.Should().Be("user1");
        config.SaslPassword.Should().Be("pass1");
        config.SslCaLocation.Should().Be("/etc/ca.pem");
        config.SslEndpointIdentificationAlgorithm.Should().Be(Confluent.Kafka.SslEndpointIdentificationAlgorithm.None);
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
        config.SecurityProtocol.Should().Be(Confluent.Kafka.SecurityProtocol.Ssl);
    }

    [Fact]
    public void BuildConsumerConfig_SetsGroupProtocolClassic_ByDefault()
    {
        var opts = new HellnetKafkaOptions();
        var config = KafkaConfigBuilder.BuildConsumerConfig(opts, "my-group");
        config.GroupProtocol.Should().Be(Confluent.Kafka.GroupProtocol.Classic);
    }

    [Fact]
    public void BuildConsumerConfig_SetsGroupProtocolConsumer_WhenConfigured()
    {
        var opts = new HellnetKafkaOptions { GroupProtocol = "consumer" };
        var config = KafkaConfigBuilder.BuildConsumerConfig(opts, "my-group");
        config.GroupProtocol.Should().Be(Confluent.Kafka.GroupProtocol.Consumer);
    }

    [Fact]
    public void BuildProducerConfig_AppendsClientIdSuffix()
    {
        var opts = new HellnetKafkaOptions { ClientId = "my-app" };
        var config = KafkaConfigBuilder.BuildProducerConfig(opts, "dlq");
        config.ClientId.Should().Be("my-app.dlq");
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
        bytes.Should().NotBeNullOrEmpty();

        var deserialized = _serializer.Deserialize<TestMessage>(bytes);
        deserialized.Should().NotBeNull();
        deserialized.Data.Should().Be("hello-kafka");
        deserialized.MessageType.Should().Be("test.message.v1");
    }

    [Fact]
    public void Serialize_ProducesSnakeCaseJson()
    {
        var message = new TestMessage { Data = "test" };
        var bytes = _serializer.Serialize(message);
        var json = System.Text.Encoding.UTF8.GetString(bytes);
        json.Should().Contain("message_type"); // snake_case
        json.Should().Contain("test.message.v1");
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

        ctx.MessageId.Should().Be("msg-123");
        ctx.Topic.Should().Be("test.topic");
        ctx.Partition.Should().Be(3);
        ctx.Offset.Should().Be(42L);
        ctx.DeliveryAttempt.Should().Be(2);
        ctx.Headers["custom-key"].Should().Be("custom-value");
    }

    [Fact]
    public void HandlesNullHeaders()
    {
        var ctx = new MessageContext("id", "topic", DateTime.UtcNow,
            new Confluent.Kafka.Partition(0), new Confluent.Kafka.Offset(0), null);

        ctx.Headers.Should().BeEmpty();
        ctx.DeliveryAttempt.Should().Be(0);
    }

    [Fact]
    public void DeliveryAttempt_ReturnsZero_WhenHeaderMissing()
    {
        var headers = new Confluent.Kafka.Headers();
        var ctx = new MessageContext("id", "topic", DateTime.UtcNow,
            new Confluent.Kafka.Partition(0), new Confluent.Kafka.Offset(0), headers);

        ctx.DeliveryAttempt.Should().Be(0);
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
        var handlerMock = new Mock<IMessageHandler<TestMessage>>();
        handlerMock.Setup(x => x.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<IMessageContext>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);
        var ctx = CreateContext();
        var message = new TestMessage();

        await engine.ExecuteAsync(handlerMock.Object, message, ctx, CancellationToken.None);

        handlerMock.Verify(x => x.HandleAsync(message, ctx, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task ExecuteAsync_Retries_ThenSucceeds()
    {
        var options = new HellnetKafkaOptions { MaxRetries = 5 };
        var engine = new RetryEngine(options, _logger, (_, _) => Task.CompletedTask);
        var handlerMock = new Mock<IMessageHandler<TestMessage>>();
        var ctx = CreateContext();
        var message = new TestMessage();
        var callCount = 0;

        handlerMock.Setup(x => x.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<IMessageContext>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Callback(() =>
            {
                callCount++;
                if (callCount <= 2)
                    throw new InvalidOperationException($"Simulated failure #{callCount}");
            });

        await engine.ExecuteAsync(handlerMock.Object, message, ctx, CancellationToken.None);

        callCount.Should().Be(3);
    }

    [Fact]
    public async Task ExecuteAsync_Throws_WhenRetriesExhausted()
    {
        var options = new HellnetKafkaOptions { MaxRetries = 2 };
        var engine = new RetryEngine(options, _logger, (_, _) => Task.CompletedTask);
        var handlerMock = new Mock<IMessageHandler<TestMessage>>();
        var ctx = CreateContext();
        var message = new TestMessage();

        handlerMock.Setup(x => x.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<IMessageContext>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("fail"));

        await engine.Invoking(e => e.ExecuteAsync(handlerMock.Object, message, ctx, CancellationToken.None))
            .Should().ThrowAsync<InvalidOperationException>();
        handlerMock.Verify(x => x.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<IMessageContext>(), It.IsAny<CancellationToken>()), Times.Exactly(2));
    }

    [Fact]
    public async Task ExecuteAsync_ThrowsOperationCanceled_WithoutRetry()
    {
        var options = new HellnetKafkaOptions { MaxRetries = 3 };
        var engine = new RetryEngine(options, _logger, (_, _) => Task.CompletedTask);
        var handlerMock = new Mock<IMessageHandler<TestMessage>>();
        var ctx = CreateContext();

        handlerMock.Setup(x => x.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<IMessageContext>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new OperationCanceledException());

        await engine.Invoking(e => e.ExecuteAsync(handlerMock.Object, new TestMessage(), ctx, CancellationToken.None))
            .Should().ThrowAsync<OperationCanceledException>();
    }

    private static IMessageContext CreateContext()
    {
        return new MessageContext("test-id", "test.topic", DateTime.UtcNow,
            new Confluent.Kafka.Partition(0), new Confluent.Kafka.Offset(0), null);
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

        sp.GetRequiredService<HellnetKafkaOptions>().Should().NotBeNull();
        sp.GetRequiredService<IMessageSerializer>().Should().NotBeNull();
        sp.GetRequiredService<IMessageBus>().Should().NotBeNull();
        sp.GetRequiredService<RetryEngine>().Should().NotBeNull();
        sp.GetRequiredService<DeadLetterService>().Should().NotBeNull();
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

        sp.GetRequiredService<HellnetKafkaOptions>().Brokers.Should().Be("test:9092");
    }

    [Fact]
    public void AddHellnetKafka_ReturnsServiceCollection()
    {
        var services = new ServiceCollection();
        var result = services.AddHellnetKafka();
        result.Should().BeSameAs(services);
    }

    [Fact]
    public void AddHellnetKafka_WithOptions_ReturnsServiceCollection()
    {
        var services = new ServiceCollection();
        var result = services.AddHellnetKafka(new HellnetKafkaOptions());
        result.Should().BeSameAs(services);
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
        handler.Should().NotBeNull();
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
        handler.Should().BeNull();
    }

    [Fact]
    public void AddHellnetKafka_RegistersJsonSerializer_ByDefault()
    {
        var services = new ServiceCollection();
        services.AddHellnetKafka(new HellnetKafkaOptions { AutoRegisterHandlers = false });
        var sp = services.BuildServiceProvider();
        sp.GetRequiredService<IMessageSerializer>().Should().BeOfType<JsonMessageSerializer>();
    }

    [Fact]
    public void AddHellnetKafka_SetsInfraBrokers()
    {
        var services = new ServiceCollection();
        services.AddHellnetKafka();
        var sp = services.BuildServiceProvider();
        var opts = sp.GetRequiredService<HellnetKafkaOptions>();
        opts.Brokers.Should().Be("kafka.hellnet.com.br:9094");
        opts.SecurityProtocol.Should().Be("sasl_ssl");
        opts.DefaultSerializer.Should().Be("avro");
        opts.TopicPrefix.Should().Be("hellnet");
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
        bus.Should().NotBeNull();
    }

    [Fact]
    public void ResolveTopic_ReturnsMessageType_WhenNoPrefix()
    {
        var options = new HellnetKafkaOptions { TopicPrefix = "" };
        var logger = NullLogger<KafkaMessageBus>.Instance;
        var serializer = new JsonMessageSerializer();
        var bus = new KafkaMessageBus(options, serializer, logger);

        var msg = new TestMessage();
        bus.ResolveTopic(msg).Should().Be("test.message.v1");

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
        bus.ResolveTopic(msg).Should().Be("hellnet.test.message.v1");

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
        service.Should().NotBeNull();
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
        host.Should().NotBeNull();
    }

    [Fact]
    public void DiscoverHandlers_FindsTestHandler()
    {
        var handlers = KafkaConsumerHost.DiscoverHandlers();
        handlers.Should().Contain(typeof(TestHandler));
    }

    [Fact]
    public void DiscoverHandlers_ReturnsHandlers_RegardlessOfOption()
    {
        var handlers = KafkaConsumerHost.DiscoverHandlers();
        handlers.Should().Contain(typeof(TestHandler));
    }

    [Fact]
    public void ResolveTopicFromMessageType_UsesMessageTypeProperty()
    {
        KafkaConsumerHost.ResolveTopicFromMessageType(typeof(TestMessage)).Should().Be("test.message.v1");
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
        result.Should().Be("ok");
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
        opts.MaxRetries.Should().Be(3);
        opts.RetryDelayMs.Should().Be(200);
        opts.RetryMaxDelayMs.Should().Be(30_000);
        opts.RetryJitterMs.Should().Be(100);
        opts.TimeoutProduceMs.Should().Be(30_000);
        opts.TimeoutSchemaRegistryMs.Should().Be(10_000);
        opts.CircuitBreakerFailureCount.Should().Be(5);
        opts.CircuitBreakerDurationMs.Should().Be(30_000);
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
        config.MessageTimeoutMs.Should().Be(15000);
        config.RequestTimeoutMs.Should().Be(15000);
    }

    [Fact]
    public void ConsumerConfig_SetsTimeouts()
    {
        var opts = new HellnetKafkaOptions { TimeoutProduceMs = 20000 };
        var config = KafkaConfigBuilder.BuildConsumerConfig(opts, "test-group");
        config.SessionTimeoutMs.Should().Be(20000);
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
        engine.Should().NotBeNull();
    }
}
