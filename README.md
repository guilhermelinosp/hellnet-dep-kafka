# Hellnet.Kafka

Opinionated Kafka integration library for .NET.

```bash
dotnet add package Hellnet.Kafka
```

## Quickstart

```csharp
// Program.cs — única linha
builder.Services.AddHellnetKafka();
```

```bash
# .env — só o que muda por serviço
HELLNET_KAFKA_CONSUMER_GROUP=hellnet.meu-app.orders
HELLNET_KAFKA_SASL_PASSWORD=hellnet2026
```

### Produzir

```csharp
public sealed record OrderCreated : IMessage
{
    public string MessageType => "order.created.v1";
    public string OrderId { get; init; } = "";
}

public class MeuService(IMessageBus bus)
{
    public async Task Criar() =>
        await bus.PublishAsync(new OrderCreated { OrderId = "123" });
}
```

### Consumir

```csharp
public class OrderHandler : IMessageHandler<OrderCreated>
{
    public Task HandleAsync(OrderCreated msg, IMessageContext ctx, CancellationToken ct)
    {
        Console.WriteLine($"Pedido {msg.OrderId} recebido");
        return Task.CompletedTask;
    }
}
```

O handler é auto-descoberto via DI.

## Configuração

| Env | Default (Hellnet) | Descrição |
|---|---|---|
| `HELLNET_KAFKA_BROKERS` | `kafka.hellnet.com.br:9094` | Lista de brokers |
| `HELLNET_KAFKA_SECURITY_PROTOCOL` | `sasl_ssl` | plaintext, ssl, sasl_plaintext, sasl_ssl |
| `HELLNET_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | PLAIN, SCRAM-SHA-256/512 |
| `HELLNET_KAFKA_SASL_USERNAME` | `hellnet-app` | Usuário SCRAM |
| `HELLNET_KAFKA_SASL_PASSWORD` | — | **Obrigatório** |
| `HELLNET_KAFKA_SSL_CA_LOCATION` | — | Path do CA cert |
| `HELLNET_KAFKA_CONSUMER_GROUP` | `""` | **Obrigatório** para consumidores |
| `HELLNET_KAFKA_TOPIC_PREFIX` | `hellnet` | Prefixo dos tópicos |
| `HELLNET_KAFKA_DEFAULT_SERIALIZER` | `avro` | json, avro |
| `HELLNET_KAFKA_SCHEMA_REGISTRY_URL` | `https://schema.hellnet.com.br` | URL do Schema Registry |
| `HELLNET_KAFKA_IDEMPOTENT` | `true` | Producer idempotente |

Com `AddHellnetKafka()`, apenas `CONSUMER_GROUP` e `SASL_PASSWORD` são obrigatórios.

## Serialização

**JSON**: `HELLNET_KAFKA_DEFAULT_SERIALIZER=json` — usa System.Text.Json, snake_case.

**Avro**: `HELLNET_KAFKA_DEFAULT_SERIALIZER=avro` — requer Schema Registry + tipos Avro (ISpecificRecord ou gerado de .avsc).

## Resiliência (Polly)

| Pipeline | Timeout | Retry | Circuit Breaker |
|---|---|---|---|
| Produce | 30s | 3x expo | 5 falhas / 30s |
| Handler | 30s | MaxRetries x expo | ❌ |
| DLQ | 30s | 3x expo | ❌ |
| Schema Registry | 10s | 2x expo | ❌ |

Configurável via `HELLNET_KAFKA_RETRY_DELAY_MS`, `HELLNET_KAFKA_TIMEOUT_PRODUCE_MS`, `HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT`, etc.

## Topic naming

```
{prefix}.{messageType}
  │         └── IMessage.MessageType
  └── HELLNET_KAFKA_TOPIC_PREFIX (default: "hellnet")
```

```
MessageType = "order.created.v1" → hellnet.order.created.v1
```

## Dead Letter Queue

Handler esgota retries → mensagem vai para `{topic}.dlq` com headers:
`dlq.reason`, `dlq.original.topic`, `dlq.original.partition`, `dlq.original.offset`.

## MessageHandlerAttribute

```csharp
[MessageHandler(Topic = "custom.topic.v1", ConsumerGroup = "hellnet.custom", MaxRetries = 5)]
public class MeuHandler : IMessageHandler<MinhaMsg> { }
```

## Testes

```bash
# Unit (45 testes)
dotnet test app/tests/Hellnet.Kafka.UnitTests/

# Integration (requer infra Hellnet)
HELLNET_KAFKA_BROKERS=192.168.1.254:9094 \
HELLNET_KAFKA_SSL_CA_LOCATION=/tmp/hellnet-ca.crt \
dotnet run --project app/tests/Hellnet.Kafka.IntegrationTests/
```

## Dependências

- .NET 10+
- Confluent.Kafka 2.15.0
- Polly.Core 8.7.0
