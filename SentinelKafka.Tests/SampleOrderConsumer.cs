using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SentinelKafka.Messaging;

namespace SentinelKafka.Tests;

// 1. Define your strong-typed message model
public class OrderMessage
{
    public string OrderId { get; set; } = string.Empty;
    public decimal Amount { get; set; }
    public string CustomerId { get; set; } = string.Empty;
}

// 2. Inherit from the enterprise KafkaConsumerBase
public class SampleOrderConsumer : KafkaConsumerBase<OrderMessage>
{
    // Define the alias mapping configured securely in appsettings.json for this environment
    protected override string TopicName => "OrdersTopicAlias";
    protected override string GroupIdName => "OrdersGroupAlias";

    public SampleOrderConsumer(
        ILogger<SampleOrderConsumer> logger,
        IOptions<KafkaOptions> kafkaConfig,
        IHostEnvironment env,
        IServiceScopeFactory scopeFactory) 
        : base(logger, kafkaConfig, env, scopeFactory)
    {
    }

    // 3. Implement your application architecture rules here!
    protected override async Task ProcessMessageAsync(OrderMessage message, CancellationToken ct)
    {
        _logger.LogInformation("Processing new order: {OrderId} for {Amount:C}", message.OrderId, message.Amount);
        
        // Simulate arbitrary heavy processing logic...
        await Task.Delay(500, ct);

        // If you explicitly throw an exception here, the Poison message handler automatically invokes SendToDlqAsync safely!
        if (message.Amount < 0)
        {
            throw new InvalidOperationException("Order amount cannot be explicitly negative!");
        }

        _logger.LogInformation("Successfully processed Order {OrderId}", message.OrderId);
    }

    // 4. Override DLQ handling to meticulously write runtime failures securely into your own independent persistence layers!
    protected override Task SendToDlqAsync(ConsumeResult<string, string> consumeResult, Exception? ex, string reason, CancellationToken ct)
    {
        _logger.LogWarning("DLQ TRIGGERED: Order payload routing gracefully to Dead Letter Queue. Reason: {Reason}. Exception: {Ex}", reason, ex?.Message);

        /*
            Since this consumer inherently operates as an infinite background singleton thread, 
            you should securely spin up isolated DbContexts scopes using your injected scope factory 
            to avoid threading collisions!
            
            using var scope = _scopeFactory.CreateScope();
            var dbContext = scope.ServiceProvider.GetRequiredService<MyApplicationDbContext>();
            
            dbContext.FailedMessages.Add(new FailedMessage {
                Topic = consumeResult.Topic,
                Payload = consumeResult.Message.Value,
                ErrorContext = ex?.ToString(),
                CreatedAt = DateTime.UtcNow
            });
            await dbContext.SaveChangesAsync(ct);
        */

        return Task.CompletedTask;
    }
}
