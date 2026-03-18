using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Timeout;

namespace SentinelKafka.Messaging;

public class ResilientKafkaProducer : IResilientKafkaProducer
{
    private readonly IKafkaProducer _kafkaProducer;
    private readonly KafkaPolicyFactory _policyFactory;
    private readonly ILogger<ResilientKafkaProducer> _logger;

    public ResilientKafkaProducer(
        IKafkaProducer kafkaProducer,
        KafkaPolicyFactory policyFactory,
        ILogger<ResilientKafkaProducer> logger)
    {
        _kafkaProducer = kafkaProducer;
        _policyFactory = policyFactory;
        _logger = logger;
    }

    public async Task<bool> ProduceWithRetryAsync<T>(string topic, T message, string? key = null, CancellationToken ct = default)
    {
        try
        {
            var retryPolicy = _policyFactory.CreateRetryPolicy();
            var circuitBreakerPolicy = _policyFactory.CreateCircuitBreakerPolicy();
            var timeoutPolicy = _policyFactory.CreateTimeoutPolicy(TimeSpan.FromSeconds(10));

            var policy = Policy.WrapAsync(timeoutPolicy, retryPolicy, circuitBreakerPolicy);

            await policy.ExecuteAsync(async (cancellationToken) =>
            {
                await _kafkaProducer.ProduceAsync(topic, message, key);
            }, ct);

            _logger.LogInformation("Successfully produced message to Kafka topic {Topic}", topic);
            return true;
        }
        catch (BrokenCircuitException ex)
        {
            _logger.LogWarning(ex, "Kafka circuit breaker is open - skipping message production to {Topic}", topic);
            return false;
        }
        catch (TimeoutRejectedException ex)
        {
            _logger.LogWarning(ex, "Kafka operation timed out for topic {Topic}", topic);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to produce message to Kafka topic {Topic} after all retries", topic);
            return false;
        }
    }
}
