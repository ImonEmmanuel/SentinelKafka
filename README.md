# SentinelKafka
A resilient .NET SDK for Confluent Kafka integration, designed for easy configuration, MSK Support, structured Policies (Polly), and Dependency Injection (DI) support.

## Features
- **Resilient Producer**: Built-in retries, circuit breakers, and timeouts via Polly.
- **AWS MSK Support**: Natively integrates with Amazon MSK using `AWS.MSK.Auth` and IAM credentials.
- **Easy DI Setup**: Use `AddSentinelKafka()` to integrate cleanly into `IServiceCollection`.
- **SOLID Design**: Clean separation of Extensions and Messaging logic.

## Installation
Install via NuGet:
```bash
dotnet add package SentinelKafka
```

Requires the following dependencies, which are brought in automatically:
- `Confluent.Kafka`
- `Polly`
- `AWS.MSK.Auth`
- `AWSSDK.Core`

## Setup & Configuration

### 1. `appsettings.json` Configuration
Add the Kafka settings to your configuration file (or map them directly via the DI configuration action):
```json
{
  "SentinelKafka": {
    "BootstrapServers": "localhost:9092",
    "Ack": 1,
    "LingerMs": 5,
    "MessageSendMaxRetries": 3,
    "IsMsk": true,
    "AccessKey": "your-aws-access-key",
    "SecretKey": "your-aws-secret-key",
    "Region": "us-east-1",
    "Topics": {
      "orders": "prod-orders-topic"
    }
  }
}
```

### 2. Dependency Injection
In your `Program.cs` or `Startup.cs`, add:

```csharp
using SentinelKafka.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Option A: Configuration via appsettings.json
builder.Services.AddSentinelKafka(options => 
    builder.Configuration.GetSection("SentinelKafka").Bind(options));

// Option B: Manual Code-Based Configuration
builder.Services.AddSentinelKafka(options =>
{
    options.IsMsk = true;
    options.AccessKey = "your-access-key";
    options.SecretKey = "your-secret-key";
    options.Region = "us-east-1";
    options.BootstrapServers = "localhost:9092";
    options.MessageSendMaxRetries = 3;
    options.Topics = new Dictionary<string, string> { { "OrdersTopicAlias", "prod-orders-topic" } };
});
```

## Usage

Inject `IResilientKafkaProducer` into your services. The DI automatically resolves to a resilient internally managed thread-safe producer wrapped via Polly policies.

```csharp
using SentinelKafka.Messaging;

public class OrderService
{
    private readonly IResilientKafkaProducer _producer;

    public OrderService(IResilientKafkaProducer producer)
    {
        _producer = producer;
    }

    public async Task CreateOrderAsync(string orderId, OrderData data)
    {
        // Produce message using Polly policies (Retry, Timeout, Circuit Breaker)
        bool success = await _producer.ProduceWithRetryAsync("orders", data, key: orderId);
        
        if (success)
        {
            Console.WriteLine("Message successfully produced to Kafka.");
        }
        else
        {
            Console.WriteLine("Failed to produce message due to timeout or open circuit.");
        }
    }
}
```

### 3. Consuming Messages (with Dead Letter Queue Support)

The `SentinelKafka` SDK provides an enterprise-ready base consumer out-of-the-box (`KafkaConsumerBase<TMessage>`). It natively handles:
- **Strongly Typed Deserialization**: Automatically parses Kafka JSON values into C# objects.
- **Robust Exception Handling**: Prevents poison-pills and gracefully captures unhandled occurrences safely.
- **Dead Letter Queue (DLQ)**: Provides an easily overridable `SendToDlqAsync` abstraction hook uniquely designed for downstream microservices to securely route pipeline failures without indefinitely stalling partition offsets.

Create a robust consuming service by inheriting this generic SDK class and overriding the processing hooks:

```csharp
using SentinelKafka.Messaging;

public class OrderMessage { public string OrderId { get; set; } }

public class OrderValidationConsumer : KafkaConsumerBase<OrderMessage>
{
    protected override string TopicName => "OrdersTopicAlias";
    protected override string GroupIdName => "OrdersGroupAlias";

    public OrderValidationConsumer(
        ILogger<OrderValidationConsumer> logger, 
        IOptions<KafkaOptions> options, 
        IHostEnvironment env, 
        IServiceScopeFactory scopeFactory)
        : base(logger, options, env, scopeFactory) { }

    protected override async Task ProcessMessageAsync(OrderMessage message, CancellationToken ct)
    {
        // Business logic sequentially orchestrated against each polled message
        Console.WriteLine($"Order Processed safely: {message.OrderId}");
    }

    // Seamlessly write failing messages & exception stacktraces directly into your application's PostgreSQL Db or specialized Topics!
    protected override async Task SendToDlqAsync(ConsumeResult<string, string> result, Exception? ex, string reason, CancellationToken ct)
    {
        using var scope = _scopeFactory.CreateScope();
        var myDb = scope.ServiceProvider.GetRequiredService<MyDbContext>();

        myDb.DeadLetters.Add(new DeadLetterRecord { 
            Payload = result.Message.Value, 
            FailureReason = reason, 
            Exception = ex?.Message 
        });
        await myDb.SaveChangesAsync(ct);
    }
}
```

Register it gracefully as a Hosted Service so it automatically kicks off during ASP.NET Core process initialization:

```csharp
builder.Services.AddHostedService<OrderValidationConsumer>();
```

### 4. Advanced Telemetry & Distributed Tracing (OpenTelemetry)
The `SentinelKafka` SDK is engineered flawlessly for decoupled microservices architectures! Both `KafkaProducer` and `KafkaConsumerBase` explicitly intercept and sequentially propagate standard W3C `traceparent` and `tracestate` payload properties inline mapping to Kafka `Headers`! 

This inherently means if your upstream HTTP API receives a request organically, the local HTTP `Activity.Current` Trace ID is natively injected synchronously onto the Kafka message object properties! This ultimately triggers a perfectly scoped, seamlessly correlated OpenTelemetry child `Activity` block on the Consumer side process dynamically ensuring tools natively monitoring your Datadog or AWS X-Ray environments successfully visualize the entire transaction saga bridging HTTP -> AWS MSK -> Background Consumer effortlessly!

### 5. High-Throughput & Confluent Schema Registry (Protobuf/Avro)
While natively serializing text utilizing `KafkaConsumerBase<TMessage>` paired cleanly with `System.Text.Json` performs brilliantly, strictly enforcing Binary schema formats precisely optimizing your payload structures implicitly like Protobuf natively averages dramatically faster deserialization speeds explicitly! 

More imperatively: leveraging Confluent Schema registries structurally securely enforces rigid static contract schemas eliminating breaking application changes actively terminating payloads from deploying incorrectly! 

To inherently leverage this logic within the SDK reliably, securely register your `SchemaRegistryUrl` directly globally on `appsettings.json`, utilize the natively registered `IProtobufKafkaProducer<>` DI abstraction effectively, or manually seamlessly inherit standard consumer instances natively extending the abstract `ProtobufKafkaConsumerBase<TMessage>` framework hook directly out-of-the-box:

```csharp
public class MyProtobufOrderConsumer : ProtobufKafkaConsumerBase<MyGeneratedProtoClass>
{
    protected override string TopicName => "OrdersTopicProto";
    protected override string GroupIdName => "GroupProto";

    public MyProtobufOrderConsumer(ILogger<MyProtobufOrderConsumer> logger, IOptions<KafkaOptions> options, IHostEnvironment env, IServiceScopeFactory scopeFactory)
        : base(logger, options, env, scopeFactory) { }

    protected override async Task ProcessMessageAsync(MyGeneratedProtoClass message, CancellationToken ct)
    {
        // Binary structures sequentially evaluating effectively!
        Console.WriteLine($"Binary schema seamlessly successfully matching valid contract inherently: {message.Id}");
    }
}
```

## License
MIT
