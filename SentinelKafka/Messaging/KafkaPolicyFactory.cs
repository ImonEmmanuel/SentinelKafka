using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.CircuitBreaker;
using Polly.Retry;
using Polly.Timeout;

namespace SentinelKafka.Messaging;

public class KafkaPolicyFactory
{
    private readonly ILogger<KafkaPolicyFactory> _logger;

    public KafkaPolicyFactory(ILogger<KafkaPolicyFactory> logger)
    {
        _logger = logger;
    }

    public AsyncRetryPolicy CreateRetryPolicy(int maxRetries = 3)
    {
        return Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(
                retryCount: maxRetries,
                sleepDurationProvider: retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                onRetry: (exception, timeSpan, retryCount, context) =>
                {
                    _logger.LogWarning(exception, 
                        "Kafka retry attempt {RetryCount} after {TimeSpan}s - Exception: {ExceptionMessage}", 
                        retryCount, timeSpan.TotalSeconds, exception.Message);
                });
    }

    public AsyncCircuitBreakerPolicy CreateCircuitBreakerPolicy(
        int exceptionsBeforeBreaking = 3, 
        TimeSpan? breakDuration = null)
    {
        breakDuration ??= TimeSpan.FromMinutes(5);
        
        return Policy
            .Handle<Exception>()
            .CircuitBreakerAsync(
                exceptionsAllowedBeforeBreaking: exceptionsBeforeBreaking,
                durationOfBreak: breakDuration.Value,
                onBreak: (exception, breakDelay) =>
                {
                    _logger.LogError(exception, 
                        "Kafka circuit breaker opened for {BreakDelay:mm\\:ss} due to: {ExceptionMessage}", 
                        breakDelay, exception.Message);
                },
                onReset: () =>
                {
                    _logger.LogInformation("Kafka circuit breaker reset - connections will be retried");
                },
                onHalfOpen: () =>
                {
                    _logger.LogInformation("Kafka circuit breaker half-open - testing connection");
                });
    }

    public AsyncTimeoutPolicy CreateTimeoutPolicy(TimeSpan timeout)
    {
        return Policy.TimeoutAsync(timeout, 
            onTimeoutAsync: (context, timeSpan, task) =>
            {
                _logger.LogWarning("Kafka operation timed out after {TimeoutSeconds}s", timeSpan.TotalSeconds);
                return Task.CompletedTask;
            });
    }
}
