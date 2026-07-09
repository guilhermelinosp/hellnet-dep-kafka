using Confluent.SchemaRegistry;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Hellnet.Kafka.Internal;
using Hellnet.Kafka.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Hellnet.Kafka;

/// <summary>
/// Extension methods for registering Hellnet.Kafka services in DI.
/// Env-first: reads HELLNET_KAFKA_* environment variables automatically.
/// </summary>
public static class DependencyInjection
{
    /// <summary>
    /// Registers Hellnet.Kafka services using environment variables.
    /// </summary>
    public static IServiceCollection AddHellnetKafka(this IServiceCollection services)
    {
        var options = KafkaEnvBinder.Bind();
        return services.AddHellnetKafka(options);
    }

    /// <summary>
    /// Registers Hellnet.Kafka services with explicit options.
    /// </summary>
    public static IServiceCollection AddHellnetKafka(
        this IServiceCollection services,
        HellnetKafkaOptions options)
    {
        // Options
        services.AddSingleton(options);

        // Schema Registry client (optional)
        if (!string.IsNullOrWhiteSpace(options.SchemaRegistryUrl))
        {
            services.AddSingleton<ISchemaRegistryClient>(sp =>
            {
                var schemaConfig = new SchemaRegistryConfig
                {
                    Url = options.SchemaRegistryUrl,
                };

                if (!string.IsNullOrWhiteSpace(options.SchemaRegistryUsername))
                {
                    schemaConfig.BasicAuthUserInfo =
                        $"{options.SchemaRegistryUsername}:{options.SchemaRegistryPassword}";
                    schemaConfig.BasicAuthCredentialsSource = AuthCredentialsSource.UserInfo;
                }

                return new CachedSchemaRegistryClient(schemaConfig);
            });
        }

        // Serializer — pick by DefaultSerializer
        services.AddSingleton<IMessageSerializer>(sp =>
        {
            var registry = sp.GetService<ISchemaRegistryClient>();
            return options.DefaultSerializer?.ToLowerInvariant() switch
            {
                "avro" when registry is not null => new AvroMessageSerializer(registry),
                _ => new JsonMessageSerializer(),
            };
        });

        // Core services
        services.AddSingleton<IMessageBus, KafkaMessageBus>();
        services.AddSingleton(sp =>
        {
            var opts = sp.GetRequiredService<HellnetKafkaOptions>();
            var logger = sp.GetRequiredService<ILogger<RetryEngine>>();
            return new RetryEngine(opts, logger);
        });
        services.AddSingleton<DeadLetterService>();
        services.AddHostedService<KafkaConsumerHost>();

        // Auto-discover and register IMessageHandler<T> implementations
        if (options.AutoRegisterHandlers)
        {
            var handlerTypes = AppDomain.CurrentDomain.GetAssemblies()
                .SelectMany(a =>
                {
                    try { return a.GetTypes(); }
                    catch { return []; }
                })
                .Where(t => t is { IsClass: true, IsAbstract: false })
                .SelectMany(t => t.GetInterfaces()
                    .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IMessageHandler<>))
                    .Select(i => new { HandlerType = t, InterfaceType = i }));

            foreach (var handler in handlerTypes)
            {
                services.TryAddTransient(handler.InterfaceType, handler.HandlerType);
                services.TryAddTransient(handler.HandlerType);
            }
        }

        return services;
    }
}
