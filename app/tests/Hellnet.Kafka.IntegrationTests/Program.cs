using Hellnet.Kafka;
using Hellnet.Kafka.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

ThreadPool.SetMinThreads(16, 16);

var exitCode = 0;
void Assert(bool ok, string msg) { if (!ok) { Console.WriteLine($"  [FAIL] {msg}"); exitCode++; } else Console.WriteLine($"  [PASS] {msg}"); }

// ═══════════════════════════════════════════════════════════
// Single test: produce AND verify in same host
// Avoids multiple-host issues with shared handler discovery
// ═══════════════════════════════════════════════════════════
Console.WriteLine("\n  Integration Test\n  " + new string('-', 20));

var group = $"hellnet.intg.all.{Guid.NewGuid().ToString("N")[..8]}";
Environment.SetEnvironmentVariable("HELLNET_KAFKA_CONSUMER_GROUP", group);

var b = Host.CreateApplicationBuilder();
b.Logging.AddConsole().SetMinimumLevel(LogLevel.Information);
b.Services.AddHellnetKafkaWithDefaults();
var host = b.Build();
await host.StartAsync();
await Task.Delay(3000);

var bus = host.Services.GetRequiredService<IMessageBus>();
var producer = host.Services.GetRequiredService<IMessageBus>();

// 1. Produce MsgA → hellnet.intg.test.a.v1
await producer.PublishAsync(new MsgA { Data = "hello" });
// 2. Produce MsgB → hellnet.intg.test.b.v1  
await producer.PublishAsync(new MsgB { Data = "prefix" });

// Wait for both handlers
await Task.Delay(10000);

Assert(CntA.Count > 0, $"MsgA handler called ({CntA.Count}x)");
Assert(CntB.Count > 0, $"MsgB handler called ({CntB.Count}x)");
Console.WriteLine($"  Counts: A={CntA.Count}, B={CntB.Count}");

await host.StopAsync();

Console.WriteLine($"\n  SUMMARY\n  {(exitCode == 0 ? "ALL TESTS PASSED  \u2705" : $"{exitCode} FAILED  \u274c")}");
return exitCode;

// ═══════════════════════════════════════════════════════════
public sealed record MsgA : IMessage { public string MessageType => "intg.test.a.v1"; public string Data { get; init; } = ""; }
public static class CntA { public static int Count; public static void Reset() => Count = 0; }
public sealed class HndA : IMessageHandler<MsgA>
{
    public Task HandleAsync(MsgA m, IMessageContext ctx, CancellationToken ct)
    { Interlocked.Increment(ref CntA.Count); Console.WriteLine($"  [HndA] received"); return Task.CompletedTask; }
}

public sealed record MsgB : IMessage { public string MessageType => "intg.test.b.v1"; public string Data { get; init; } = ""; }
public static class CntB { public static int Count; public static void Reset() => Count = 0; }
public sealed class HndB : IMessageHandler<MsgB>
{
    public Task HandleAsync(MsgB m, IMessageContext ctx, CancellationToken ct)
    { Interlocked.Increment(ref CntB.Count); Console.WriteLine($"  [HndB] received"); return Task.CompletedTask; }
}
