# Hellnet.Kafka

Opinionated Kafka integration library for .NET applications.  
Built on [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) with Schema Registry support.

## Quickstart

```bash
dotnet add package Hellnet.Kafka
```

### Minimal setup (infra Hellnet)

```csharp
// Program.cs — única linha
builder.Services.AddHellnetKafka();
```

```bash
# .env — só o que muda por serviço
HELLNET_KAFKA_CONSUMER_GROUP=hellnet.meu-app.orders
HELLNET_KAFKA_SASL_PASSWORD=hellnet2026
```

### Produzir mensagem

```csharp
public sealed record OrderCreated : IMessage
{
    public string MessageType => "order.created.v1";
    public string OrderId { get; init; } = "";
    public double Total { get; init; }
}

public class MeuService
{
    private readonly IMessageBus _bus;

    public MeuService(IMessageBus bus) => _bus = bus;

    public async Task CriarPedido()
    {
        await _bus.PublishAsync(new OrderCreated
        {
            OrderId = Guid.NewGuid().ToString(),
            Total = 299.90,
        });
    }
}
```

### Consumir mensagem

```csharp
public class OrderCreatedHandler : IMessageHandler<OrderCreated>
{
    private readonly ILogger<OrderCreatedHandler> _logger;

    public OrderCreatedHandler(ILogger<OrderCreatedHandler> logger)
        => _logger = logger;

    public Task HandleAsync(OrderCreated message, IMessageContext context, CancellationToken ct)
    {
        _logger.LogInformation("Pedido {OrderId} recebido", message.OrderId);
        return Task.CompletedTask;
    }
}
```

O handler é **auto-descoberto** via DI — só precisa estar registrado no container.

### Exemplo completo (.NET Worker)

```csharp
using Hellnet.Kafka;
using Hellnet.Kafka.Abstractions;

var builder = Host.CreateApplicationBuilder(args);
builder.Services.AddHellnetKafka();
builder.Services.AddTransient<IMessageHandler<OrderCreated>, OrderCreatedHandler>();

var host = builder.Build();
await host.RunAsync();
```

---

## Configuração

### Environment variables

| Env | Default (Hellnet infra) | Descrição |
|---|---|---|
| `HELLNET_KAFKA_BROKERS` | `kafka.hellnet.com.br:9094` | Lista de brokers |
| `HELLNET_KAFKA_SECURITY_PROTOCOL` | `sasl_ssl` | `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `HELLNET_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `HELLNET_KAFKA_SASL_USERNAME` | `hellnet-app` | Usuário SCRAM |
| `HELLNET_KAFKA_SASL_PASSWORD` | — | **Obrigatório** (sem default seguro) |
| `HELLNET_KAFKA_SSL_CA_LOCATION` | — | Path do CA cert para verificação TLS |
| `HELLNET_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | `""` | Vazio = desliga hostname verification |
| `HELLNET_KAFKA_CONSUMER_GROUP` | `""` | **Obrigatório** para consumidores |
| `HELLNET_KAFKA_TOPIC_PREFIX` | `hellnet` | Prefixo dos tópicos |
| `HELLNET_KAFKA_GROUP_PROTOCOL` | `classic` | `classic` ou `consumer` (KIP-848) |
| `HELLNET_KAFKA_DEFAULT_SERIALIZER` | `avro` | `json`, `avro` |
| `HELLNET_KAFKA_SCHEMA_REGISTRY_URL` | `https://schema.hellnet.com.br` | URL do Schema Registry |
| `HELLNET_KAFKA_IDEMPOTENT` | `true` | Producer idempotente |
| `HELLNET_KAFKA_AUTO_OFFSET_RESET` | `earliest` | `earliest`, `latest`, `none` |
| `HELLNET_KAFKA_MAX_RETRIES` | `3` | Retries antes do DLQ |
| `HELLNET_KAFKA_DEAD_LETTER_TOPIC` | `{topic}.dlq` | Tópico de dead-letter |

**Nota**: Com `AddHellnetKafka()`, apenas `HELLNET_KAFKA_CONSUMER_GROUP` e `HELLNET_KAFKA_SASL_PASSWORD` são obrigatórios por serviço.

### Config padrão (sem infra Hellnet)

```csharp
services.AddHellnetKafka();
```

Lê apenas `HELLNET_KAFKA_*` environment vars. Default seguro para ambientes não-Hellnet.

---

## Arquitetura

```
App
 │
 ├── IMessageBus.PublishAsync<T>()     → Producer (SASL_SSL)
 │
 └── KafkaConsumerHost (BackgroundService)
      │
      └── IMessageHandler<T>.HandleAsync()
           │
           ├── RetryEngine (exponential backoff)
           │
           └── DeadLetterService (DLQ topic)
```

### Fluxo de consumo

1. `KafkaConsumerHost` descobre todos os `IMessageHandler<T>` registrados
2. Cada handler ganha um consumer próprio com `groupId = consumerGroup`
3. Mensagem recebida → deserializa → chama `HandleAsync`
4. Se lançar exceção → `RetryEngine` tenta novamente (exponential backoff)
5. Se esgotar retries → `DeadLetterService` publica no tópico `.dlq`
6. Offset é commitado apenas após sucesso ou DLQ

---

## Serialização

### JSON (padrão)

```csharp
HELLNET_KAFKA_DEFAULT_SERIALIZER=json
```

Usa `System.Text.Json` com `SnakeCaseLower`. Não precisa de Schema Registry.

### Avro

```csharp
HELLNET_KAFKA_DEFAULT_SERIALIZER=avro
```

Usa `AvroSerializer<T>` do Confluent com Schema Registry.  
**Requer** `HELLNET_KAFKA_SCHEMA_REGISTRY_URL` configurado.

O tipo da mensagem precisa ser um **Avro-specific record** (gerado de schema `.avsc`):

```csharp
// Gerado pelo Confluent.Avro.CodeGen a partir de:
// {
//   "namespace": "com.hellnet.orders",
//   "type": "record",
//   "name": "OrderCreated",
//   "fields": [
//     {"name": "orderId", "type": "string"},
//     {"name": "total", "type": "double"}
//   ]
// }
public partial class OrderCreated : ISpecificRecord { ... }
```

---

## Tópicos e nomenclatura

### Convenção

```
{prefix}.{messageType}
  │         └── definido por IMessage.MessageType
  └── HELLNET_KAFKA_TOPIC_PREFIX (default: "hellnet")
```

Exemplos com prefixo `hellnet`:

| MessageType | Tópico real |
|---|---|
| `order.created.v1` | `hellnet.order.created.v1` |
| `invoice.paid.v1` | `hellnet.invoice.paid.v1` |
| `stock.updated.v1` | `hellnet.stock.updated.v1` |

### Consumer group

```
hellnet.{app}.{domain}.{event}
```

Definido por `HELLNET_KAFKA_CONSUMER_GROUP` no serviço.

---

## Dead Letter Queue

Quando um handler esgota as tentativas de retry, a mensagem é publicada no tópico `{topic}.dlq` com headers:

| Header | Descrição |
|---|---|
| `dlq.reason` | Motivo da falha |
| `dlq.original.topic` | Tópico original |
| `dlq.original.partition` | Partição original |
| `dlq.original.offset` | Offset original |

---

## MessageHandlerAttribute

Permite configurar o consumo por handler:

```csharp
[MessageHandler(
    Topic = "custom.topic.v1",           // tópico fixo
    ConsumerGroup = "hellnet.custom",     // grupo específico
    MaxRetries = 5                        // sobrescreve default
)]
public class MeuHandler : IMessageHandler<MinhaMessage>
{
    // ...
}
```

---

## Testes

```bash
# Unit tests (45 testes)
dotnet test app/tests/Hellnet.Kafka.Tests/
```

Testes de integração contra a infra Hellnet estão em `/tmp/hellnet-kafka-test`
(requerem acesso à rede Hellnet e certificado TLS).

---

## Dependências

- .NET 10+
- Confluent.Kafka 2.15.0
- Confluent.SchemaRegistry 2.15.0
- Confluent.SchemaRegistry.Serdes.Avro 2.15.0
- Confluent.SchemaRegistry.Serdes.Json 2.15.0
- Confluent.SchemaRegistry.Serdes.Protobuf 2.15.0
- Microsoft.Extensions.DependencyInjection
- Microsoft.Extensions.Hosting
