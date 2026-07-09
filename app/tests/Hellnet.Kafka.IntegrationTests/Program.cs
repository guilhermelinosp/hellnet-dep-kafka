using Hellnet.Kafka;
using Hellnet.Kafka.Abstractions;
using Hellnet.Kafka.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("HELLNET_KAFKA_BROKERS")))
{
    Console.WriteLine("SKIP: HELLNET_KAFKA_BROKERS not set");
    return 0;
}

var exitCode = 0;
void Assert(string name, bool ok, string? msg = null)
{
    if (ok) Console.WriteLine($"  [PASS] {name}");
    else { Console.WriteLine($"  [FAIL] {name}"); if (msg is not null) Console.WriteLine($"         {msg}"); exitCode++; }
}

string Group(string suffix) => $"hellnet.intg.{suffix}.{Guid.NewGuid().ToString("N")[..8]}";

await Test("1. Produce + Consume (JSON)", RunJsonTest);
await Test("2. Topic prefix", RunPrefixTest);

Console.WriteLine($"\n  SUMMARY");
if (exitCode == 0) Console.WriteLine("  ALL TESTS PASSED  ✅");
else Console.WriteLine($"  {exitCode} TEST(S) FAILED  ❌");
return exitCode;

// ═══════════════════════════════════════════════════════════
async Task Test(string name, Func<Task> fn)
{
    Console.Write($"\n  {name}... ");
    try { await fn(); Console.WriteLine("  ✅ PASS"); }
    catch (Exception ex) { Console.WriteLine($"  ❌ FAIL: {ex.Message}"); exitCode++; }
}

// ═══════════════════════════════════════════════════════════
// 1. JSON produce + consume
// ═══════════════════════════════════════════════════════════
async Task RunJsonTest()
{
    Counter.Reset();

    var group = Group("json");
    using var host = BuildHost(group, services =>
    {
        services.AddTransient<IMessageHandler<JsonMsg>, JsonHandler>();
    });
    await host.StartAsync();
    await Task.Delay(2000);

    var bus = host.Services.GetRequiredService<IMessageBus>();
    await bus.PublishAsync(new JsonMsg { Data = "hello" });
    await WaitFor(() => Counter.Count > 0, 10);
    Assert("Received message", Counter.Count > 0, $"got {Counter.Count}");

    await host.StopAsync();
}

// ═══════════════════════════════════════════════════════════
// 2. Topic prefix
// ═══════════════════════════════════════════════════════════
async Task RunPrefixTest()
{
    Counter.Reset();
    Environment.SetEnvironmentVariable("HELLNET_KAFKA_TOPIC_PREFIX", "hellnet");

    var group = Group("prefix");
    using var host = BuildHost(group, services =>
    {
        services.AddTransient<IMessageHandler<PrefixMsg>, PrefixHandler>();
    });
    await host.StartAsync();
    await Task.Delay(2000);

    var bus = host.Services.GetRequiredService<IMessageBus>();
    await bus.PublishAsync(new PrefixMsg { Data = "prefix-test" });
    await WaitFor(() => Counter.Count > 0, 10);
    Assert("Received on prefixed topic", Counter.Count > 0);

    await host.StopAsync();
}

// ═══════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════
IHost BuildHost(string group, Action<IServiceCollection> registerHandlers)
{
    Environment.SetEnvironmentVariable("HELLNET_KAFKA_CONSUMER_GROUP", group);
    var b = Host.CreateApplicationBuilder();
    b.Logging.ClearProviders();
    b.Services.AddHellnetKafkaWithDefaults();
    registerHandlers(b.Services);
    return b.Build();
}

async Task WaitFor(Func<bool> check, int timeoutSecs)
{
    var end = DateTime.UtcNow.AddSeconds(timeoutSecs);
    while (DateTime.UtcNow < end)
    {
        if (check()) return;
        await Task.Delay(500);
    }
    throw new TimeoutException($"Condition not met within {timeoutSecs}s");
}

// ═══════════════════════════════════════════════════════════
// Types
// ═══════════════════════════════════════════════════════════
public static class Counter { public static int Count; public static void Reset() => Count = 0; }

// Test 1: JSON basic
public sealed record JsonMsg : IMessage { public string MessageType => "test.json.v1"; public string Data { get; init; } = ""; }
public sealed class JsonHandler : IMessageHandler<JsonMsg>
{
    public Task HandleAsync(JsonMsg m, IMessageContext ctx, CancellationToken ct)
    { Interlocked.Increment(ref Counter.Count); return Task.CompletedTask; }
}

// Test 2: Topic prefix
public sealed record PrefixMsg : IMessage { public string MessageType => "test.prefix.v1"; public string Data { get; init; } = ""; }
public sealed class PrefixHandler : IMessageHandler<PrefixMsg>
{
    public Task HandleAsync(PrefixMsg m, IMessageContext ctx, CancellationToken ct)
    { Interlocked.Increment(ref Counter.Count); return Task.CompletedTask; }
}
