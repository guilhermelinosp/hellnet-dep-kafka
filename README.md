# Hellnet.Kafka

[![NuGet](https://img.shields.io/nuget/v/Hellnet.Kafka)](https://www.nuget.org/packages/Hellnet.Kafka)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Biblioteca opinionada de integração Kafka para microsserviços .NET.  
Construída sobre [Confluent.Kafka](https://github.com/confluentinc/confluent-kafka-dotnet) com resiliência via [Polly](https://www.pollydocs.org/).

```bash
dotnet add package Hellnet.Kafka
```

---

- [Quickstart](#quickstart)
- [Serialização](#serialização)
- [Resiliência](#resiliência)
- [Topic naming](#topic-naming)
- [Dead Letter Queue](#dead-letter-queue)
- [MessageHandlerAttribute](#messagehandlerattribute)
- [Configuração](#configuração)
- [Arquitetura](#arquitetura)
- [ADR — Decisões Técnicas](#adr--decisões-técnicas)
- [Testes](#testes)

---

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
    public double Total { get; init; }
}

public class MeuService(IMessageBus bus)
{
    public async Task Criar() =>
        await bus.PublishAsync(new OrderCreated
        {
            OrderId = "123",
            Total = 299.90
        });
}
```

### Consumir

```csharp
public class OrderHandler : IMessageHandler<OrderCreated>
{
    private readonly ILogger<OrderHandler> _logger;

    public OrderHandler(ILogger<OrderHandler> logger) => _logger = logger;

    public Task HandleAsync(OrderCreated msg, IMessageContext ctx, CancellationToken ct)
    {
        _logger.LogInformation("Pedido {OrderId} recebido (partição {P}, offset {O})",
            msg.OrderId, ctx.Partition, ctx.Offset);
        return Task.CompletedTask;
    }
}
```

O handler é **auto-descoberto** via DI — apenas defina a classe que a lib registra e inicia o consumer automaticamente.

---

## Serialização

### JSON

```bash
HELLNET_KAFKA_DEFAULT_SERIALIZER=json
```

Usa `System.Text.Json` com `SnakeCaseLower`. Não precisa de Schema Registry.  
Recomendado para desenvolvimento local e mensagens simples.

### Avro

```bash
HELLNET_KAFKA_DEFAULT_SERIALIZER=avro
HELLNET_KAFKA_SCHEMA_REGISTRY_URL=https://schema.hellnet.com.br
```

Usa `AvroSerializer<T>` do Confluent com Schema Registry.  
Requer tipos Avro (`ISpecificRecord`) gerados de arquivos `.avsc`.

```csharp
// Schema registrado no Apicurio Registry (hellnet-dep-schema)
[AvroSchema(EmbeddedResource = "Schemas.order-created.avsc")]
public sealed record OrderCreated : IMessage
{
    public string MessageType => "order.created.v1";
}
```

### Protobuf

```bash
HELLNET_KAFKA_DEFAULT_SERIALIZER=protobuf
```

Usa `ProtobufSerializer<T>` do Confluent com Schema Registry.

---

## Resiliência

Todas as operações são envolvidas por pipelines Polly compostos por **Timeout → Retry → Circuit Breaker**:

| Pipeline | Timeout | Retry | Circuit Breaker |
|----------|---------|-------|-----------------|
| **Produce** | 30s | 3x expo + jitter | 5 falhas / 30s |
| **Handler** | 30s | Até `MaxRetries` (3x) | ❌ |
| **DLQ** | 30s | 3x expo | ❌ |
| **Schema Registry** | 10s | 2x expo | ❌ |

Configurável por env vars (`HELLNET_KAFKA_RETRY_DELAY_MS`, `HELLNET_KAFKA_TIMEOUT_PRODUCE_MS`, `HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT`, etc).

### Fluxo de falha (Produce)

```
ProduceAsync falha (broker timeout)
  → Timeout 30s → estoura
  → Retry 1 (200ms + jitter)
  → Retry 2 (400ms + jitter)
  → Circuit breaker conta +1 (5/5 → ABERTO)
  → Próximos produce por 30s: falham instantaneamente
  → Após 30s: half-open → testa → se ok → FECHA
```

---

## Topic naming

```
{prefix}.{messageType}
  │         └── IMessage.MessageType
  └── HELLNET_KAFKA_TOPIC_PREFIX (default: "hellnet")
```

| MessageType | Tópico final |
|---|---|
| `order.created.v1` | `hellnet.order.created.v1` |
| `invoice.paid.v1` | `hellnet.invoice.paid.v1` |
| `stock.updated.v1` | `hellnet.stock.updated.v1` |

Consumer group segue o padrão `hellnet.{app}.{dominio}.{evento}`.

---

## Dead Letter Queue

Quando um handler esgota os retries, a mensagem é publicada no tópico `{topic}.dlq`:

| Header | Descrição |
|--------|-----------|
| `dlq.reason` | Motivo da falha |
| `dlq.original.topic` | Tópico original |
| `dlq.original.partition` | Partição original |
| `dlq.original.offset` | Offset original |

---

## MessageHandlerAttribute

```csharp
[MessageHandler(Topic = "custom.topic.v1", ConsumerGroup = "hellnet.custom", MaxRetries = 5)]
public class MeuHandler : IMessageHandler<MinhaMsg> { }
```

Sobrescreve o tópico, grupo e número de retries por handler.

---

## Configuração

### Env vars

| Env | Default | Descrição |
|-----|---------|-----------|
| `HELLNET_KAFKA_BROKERS` | `kafka.hellnet.com.br:9094` | Lista de brokers |
| `HELLNET_KAFKA_SECURITY_PROTOCOL` | `sasl_ssl` | plaintext, ssl, sasl_plaintext, sasl_ssl |
| `HELLNET_KAFKA_SASL_MECHANISM` | `SCRAM-SHA-512` | PLAIN, SCRAM-SHA-256/512 |
| `HELLNET_KAFKA_SASL_USERNAME` | `hellnet-app` | Usuário SCRAM |
| `HELLNET_KAFKA_SASL_PASSWORD` | — | **Obrigatório** |
| `HELLNET_KAFKA_SSL_CA_LOCATION` | — | Path do CA cert |
| `HELLNET_KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM` | `""` | Vazio = desliga hostname verification |
| `HELLNET_KAFKA_CONSUMER_GROUP` | `""` | **Obrigatório** para consumidores |
| `HELLNET_KAFKA_TOPIC_PREFIX` | `hellnet` | Prefixo dos tópicos |
| `HELLNET_KAFKA_GROUP_PROTOCOL` | `classic` | classic, consumer (KIP-848) |
| `HELLNET_KAFKA_DEFAULT_SERIALIZER` | `avro` | json, avro, protobuf |
| `HELLNET_KAFKA_SCHEMA_REGISTRY_URL` | `https://schema.hellnet.com.br` | URL do Schema Registry |
| `HELLNET_KAFKA_IDEMPOTENT` | `true` | Producer idempotente |
| `HELLNET_KAFKA_MAX_RETRIES` | `3` | Total de tentativas (handler) |
| `HELLNET_KAFKA_RETRY_DELAY_MS` | `200` | Delay base (exponential backoff) |
| `HELLNET_KAFKA_TIMEOUT_PRODUCE_MS` | `30000` | Timeout de produce |
| `HELLNET_KAFKA_TIMEOUT_SCHEMA_REGISTRY_MS` | `10000` | Timeout Schema Registry |
| `HELLNET_KAFKA_CIRCUIT_BREAKER_COUNT` | `5` | Falhas antes de abrir o circuit breaker |
| `HELLNET_KAFKA_DEAD_LETTER_TOPIC` | `{topic}.dlq` | Tópico de dead-letter |

Com `AddHellnetKafka()`, apenas `HELLNET_KAFKA_CONSUMER_GROUP` e `HELLNET_KAFKA_SASL_PASSWORD` são obrigatórios por serviço.

### Config sem defaults Hellnet

```csharp
services.AddHellnetKafka(new HellnetKafkaOptions
{
    Brokers = "localhost:9092",
    SecurityProtocol = "plaintext",
});
```

---

## Arquitetura

```
App
 │
 ├── IMessageBus.PublishAsync<T>()     → Producer (SASL_SSL)
 │                                        resiliência via Polly
 │
 └── KafkaConsumerHost (BackgroundService)
      │
      └── IMessageHandler<T>.HandleAsync()
           │
           ├── Polly Retry Pipeline (expo backoff + jitter)
           │
           └── DeadLetterService (DLQ topic)
```

### Fluxo de consumo

1. `KafkaConsumerHost` descobre todos os `IMessageHandler<T>` no assembly (auto-discover)
2. Cada handler ganha um consumer próprio com grupo dedicado
3. Mensagem recebida → deserializa → executa `HandleAsync`
4. Se lançar exceção → Polly retenta (exponential backoff com jitter)
5. Se esgotar retries → `DeadLetterService` publica no tópico `.dlq` (com resiliência)
6. Offset é commitado apenas após sucesso ou DLQ

### Estrutura do projeto

```
app/
├── Hellnet.Kafka.slnx
├── src/Hellnet.Kafka/
│   ├── Abstractions/         # Interfaces: IMessage, IMessageBus, IMessageHandler
│   ├── Configuration/        # Options, defaults, env binder, config builder
│   ├── Internal/             # KafkaMessageBus, KafkaConsumerHost, RetryEngine, DLQ
│   ├── Serialization/        # IMessageSerializer, Json, Avro
│   └── DependencyInjection.cs
├── tests/Hellnet.Kafka.UnitTests/       # 55 testes (xUnit, FluentAssertions, Moq)
└── tests/Hellnet.Kafka.IntegrationTests/ # 2 testes (console app, Kafka real)
```

---

## ADR — Decisões Técnicas

As decisões arquiteturais da biblioteca estão documentadas no formato ADR (Architecture Decision Record):

**Gist**: https://gist.github.com/guilhermelinosp/445a7b091f03abcca16839f342dab244

| # | Decisão | Resumo |
|---|---------|--------|
| 1 | **Env-first** | `AddHellnetKafka()` com defaults da infra, sobrescrita por env vars |
| 2 | **Auto-discover** | `IMessageHandler<T>` é descoberto automaticamente via assembly scan |
| 3 | **Topic naming** | `{prefix}.{messageType}` — padrão consistente entre serviços |
| 4 | **DLQ** | `{topic}.dlq` com headers de origem |
| 5 | **Polly pipelines** | Timeout + Retry + Circuit Breaker em produce, handler, DLQ, Schema Registry |
| 6 | **RetryEngine com Polly** | Substitui while-loop manual por pipeline testado |
| 7 | **Schema Registry** | Avro/JSON/Protobuf via Confluent Serdes + Apicurio |
| 8 | **LongRunning tasks** | Cada consumer em thread dedicada (TaskCreationOptions.LongRunning) |

---

## Testes

```bash
# Unit (55 testes, 90% coverage)
dotnet test app/tests/Hellnet.Kafka.UnitTests/

# Coverage com threshold
dotnet test app/tests/Hellnet.Kafka.UnitTests/ -c release \
  /p:CollectCoverage=true /p:Exclude="[*Tests]*" \
  /p:Threshold=90 /p:ThresholdType=line /p:ThresholdStat=total

# Integration (requer infra Hellnet real)
HELLNET_KAFKA_BROKERS=192.168.1.254:9094 \
HELLNET_KAFKA_SSL_CA_LOCATION=/tmp/hellnet-ca.crt \
dotnet run --project app/tests/Hellnet.Kafka.IntegrationTests/
```

### Libs de teste

| Pacote | Uso |
|--------|-----|
| `xunit` | Test runner |
| `FluentAssertions` | Asserts legíveis: `x.Should().Be(y)` |
| `Moq` | Mocks: `new Mock<IMessageHandler>()` |
| `AutoFixture` | Dados automáticos: `new Fixture().Create<T>()` |
| `AutoFixture.AutoMoq` | Auto-mock via AutoFixture |
| `coverlet.msbuild` | Coverage integrado ao `dotnet test` |

---

## Dependências

| Pacote | Versão | Uso |
|--------|--------|-----|
| Confluent.Kafka | 2.15.0 | Cliente Kafka |
| Confluent.SchemaRegistry | 2.15.0 | Schema Registry client |
| Confluent.SchemaRegistry.Serdes.Avro | 2.15.0 | Serialização Avro |
| Confluent.SchemaRegistry.Serdes.Json | 2.15.0 | Serialização JSON Schema |
| Confluent.SchemaRegistry.Serdes.Protobuf | 2.15.0 | Serialização Protobuf |
| Polly.Core | 8.7.0 | Resiliência (retry, timeout, circuit breaker) |
| Microsoft.Extensions.DependencyInjection.Abstractions | 10.0 | DI |
| Microsoft.Extensions.Hosting.Abstractions | 10.0 | BackgroundService |

---

## Licença

Apache 2.0 © 2026 Hellnet
