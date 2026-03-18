# SentinelKafka
A resilient .NET SDK for Confluent Kafka integration, designed for easy configuration, MSK Support, structured Policies (Polly), and Dependency Injection (DI) support.

## Features
- **Resilient Producer**: Built-in retries, circuit breakers, and timeouts via Polly.
- **AWS MSK Support**: Natively integrates with Amazon MSK using `AWS.MSK.Auth` and IAM credentials.
- **Easy DI Setup**: Use `AddKafkaProducer()` to integrate cleanly into `IServiceCollection`.
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

// Register SentinelKafka dependencies
builder.Services.AddKafkaProducer(options =>
{
    // Bind from appsettings.json
    builder.Configuration.GetSection("SentinelKafka").Bind(options);
    
    // Or configure them manually:
    // options.IsMsk = false;
    // options.BootstrapServers = "localhost:9092";
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

## License
MIT
