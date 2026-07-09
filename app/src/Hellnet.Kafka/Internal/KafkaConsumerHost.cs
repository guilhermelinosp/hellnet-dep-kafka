using System.Diagnostics.CodeAnalysis;
using Confluent.Kafka;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Hellnet.Kafka.Internal;

/// <summary>
/// Background service that consumes messages from Kafka and dispatches to registered handlers.
/// Each handler type gets its own consumer instance.
/// </summary>
internal sealed class KafkaConsumerHost(
    HellnetKafkaOptions options,
    IServiceProvider serviceProvider,
    ILogger<KafkaConsumerHost> logger)
    : BackgroundService
{
    [ExcludeFromCodeCoverage]
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var handlerTypes = DiscoverHandlers();
        if (handlerTypes.Count == 0)
        {
            logger.LogInformation("No IMessageHandler implementations found. Consumer host idle.");
            return;
        }

        var tasks = handlerTypes.Select(handlerType =>
            ConsumeLoop(handlerType, stoppingToken));

        await Task.WhenAll(tasks);
    }

    internal List<Type> DiscoverHandlers()
    {
        if (!options.AutoRegisterHandlers)
            return [];

        // Scan assembly for IMessageHandler<T> implementations
        return AppDomain.CurrentDomain.GetAssemblies()
            .SelectMany(a => a.GetTypes())
            .Where(t => t is { IsClass: true, IsAbstract: false })
            .Where(t => t.GetInterfaces().Any(i =>
                i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<>)))
            .ToList();
    }

    [ExcludeFromCodeCoverage]
    private async Task ConsumeLoop(Type handlerType, CancellationToken stoppingToken)
    {
        var messageType = handlerType.GetInterfaces()
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
            .GetGenericArguments()[0];

        var attr = handlerType.GetCustomAttributes(typeof(MessageHandlerAttribute), true)
            .Cast<MessageHandlerAttribute>()
            .FirstOrDefault();

        var topic = attr?.Topic ?? ResolveTopicFromMessageType(messageType);
        var groupId = attr?.ConsumerGroup ?? options.ConsumerGroup;
        var maxRetries = attr?.MaxRetries ?? options.MaxRetries;

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = options.Brokers,
            GroupId = groupId,
            AutoOffsetReset = options.AutoOffsetReset?.ToLowerInvariant() switch
            {
                "earliest" => Confluent.Kafka.AutoOffsetReset.Earliest,
                "latest" => Confluent.Kafka.AutoOffsetReset.Latest,
                _ => Confluent.Kafka.AutoOffsetReset.Earliest,
            },
            EnableAutoCommit = false,
            ClientId = $"{options.ClientId}.{groupId}",
        };

        if (!string.IsNullOrWhiteSpace(options.SaslMechanism))
        {
            consumerConfig.SecurityProtocol = SecurityProtocol.SaslPlaintext;
            consumerConfig.SaslMechanism = options.SaslMechanism switch
            {
                "PLAIN" => SaslMechanism.Plain,
                "SCRAM-SHA-256" => SaslMechanism.ScramSha256,
                "SCRAM-SHA-512" => SaslMechanism.ScramSha512,
                "GSSAPI" => SaslMechanism.Gssapi,
                "OAUTHBEARER" => SaslMechanism.OAuthBearer,
                _ => null,
            };
            consumerConfig.SaslUsername = options.SaslUsername;
            consumerConfig.SaslPassword = options.SaslPassword;
        }

        using var consumer = new ConsumerBuilder<string, byte[]>(consumerConfig).Build();
        consumer.Subscribe(topic);

        logger.LogInformation(
            "Consumer started for {Topic} (group: {GroupId}, handler: {Handler})",
            topic, groupId, handlerType.Name);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(stoppingToken);

                    using var scope = serviceProvider.CreateScope();
                    var handler = scope.ServiceProvider.GetRequiredService(handlerType);
                    var serializer = scope.ServiceProvider.GetRequiredService<IMessageSerializer>();
                    var retryEngine = scope.ServiceProvider.GetRequiredService<RetryEngine>();
                    var dlq = scope.ServiceProvider.GetRequiredService<DeadLetterService>();

                    var message = Deserialize(consumeResult.Message.Value, messageType, serializer);
                    var context = new MessageContext(
                        consumeResult.Message.Key,
                        consumeResult.Topic,
                        consumeResult.Message.Timestamp.UtcDateTime,
                        consumeResult.Partition,
                        consumeResult.Offset,
                        consumeResult.Message.Headers);

                    var handleMethod = typeof(IMessageHandler<>)
                        .MakeGenericType(messageType)
                        .GetMethod(nameof(IMessageHandler<IMessage>.HandleAsync))!;

                    var retryMethod = typeof(RetryEngine)
                        .GetMethod(nameof(RetryEngine.ExecuteAsync))!
                        .MakeGenericMethod(messageType);

                    try
                    {
                        await (Task)retryMethod.Invoke(retryEngine, [handler, message, context, stoppingToken])!;
                        consumer.Commit(consumeResult);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        await dlq.SendToDeadLetterAsync(
                            (IMessage)message,
                            context,
                            $"Retry exhausted: {ex.Message}",
                            stoppingToken);
                        consumer.Commit(consumeResult);
                    }
                }
                catch (ConsumeException ex)
                {
                    logger.LogError(ex, "Consume error on {Topic}", topic);
                }
            }
        }
        finally
        {
            consumer.Close();
            logger.LogInformation("Consumer stopped for {Topic}", topic);
        }
    }

    internal static string ResolveTopicFromMessageType(Type messageType)
    {
        // Convention: if message type has a static MessageType property via IMessage,
        // create an instance to get it. Otherwise use type name.
        try
        {
            var instance = Activator.CreateInstance(messageType) as IMessage;
            return instance?.MessageType ?? messageType.Name;
        }
        catch
        {
            return messageType.Name;
        }
    }

    [ExcludeFromCodeCoverage]
    private static object Deserialize(byte[] data, Type messageType, IMessageSerializer serializer)
    {
        var method = typeof(IMessageSerializer)
            .GetMethod(nameof(IMessageSerializer.Deserialize))!
            .MakeGenericMethod(messageType);

        return method.Invoke(serializer, [data])!;
    }
}
