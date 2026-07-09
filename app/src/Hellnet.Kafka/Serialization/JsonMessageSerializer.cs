using System.Text.Json;
using Hellnet.Kafka.Abstractions;

namespace Hellnet.Kafka.Serialization;

/// <summary>
/// Default JSON serializer using System.Text.Json.
/// </summary>
public sealed class JsonMessageSerializer : IMessageSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = false,
    };

    public byte[] Serialize<TMessage>(TMessage message)
        where TMessage : IMessage
        => JsonSerializer.SerializeToUtf8Bytes(message, Options);

    public TMessage Deserialize<TMessage>(byte[] data)
        where TMessage : IMessage
        => JsonSerializer.Deserialize<TMessage>(data, Options)
           ?? throw new InvalidOperationException($"Deserialization returned null for {typeof(TMessage).Name}");
}
