using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using AWS.MSK.Auth;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SentinelKafka.Messaging;

public abstract class KafkaConsumerBase<TMessage> : BackgroundService where TMessage : class
{
    protected readonly ILogger _logger;
    protected readonly AWSMSKAuthTokenGenerator _tokenGenerator = new AWSMSKAuthTokenGenerator();

    protected readonly KafkaOptions _kafkaConfig;
    protected readonly IHostEnvironment _env;
    protected readonly IServiceScopeFactory _scopeFactory;

    protected readonly string _topic;
    protected readonly string _groupId;

    protected abstract string TopicName { get; }
    protected abstract string GroupIdName { get; }
    
    /// <summary>
    /// Implemented by consuming service to handle the strongly-typed deserialized message.
    /// </summary>
    protected abstract Task ProcessMessageAsync(TMessage message, CancellationToken ct);

    /// <summary>
    /// Invoked when a message fails validation, JSON deserialization, or when ProcessMessageAsync throws an unhandled exception.
    /// The consuming service leveraging this SDK should override this to write to their specific DLQ (e.g. Postgres DB, API, DLQ Topic).
    /// </summary>
    protected virtual Task SendToDlqAsync(ConsumeResult<string, string> consumeResult, Exception? ex, string reason, CancellationToken ct)
    {
        _logger.LogWarning("Message automatically routed to DLQ path but no DLQ handler was overridden. Reason: {Reason}", reason);
        return Task.CompletedTask;
    }

    protected KafkaConsumerBase(
        ILogger logger,
        IOptions<KafkaOptions> kafkaConfig,
        IHostEnvironment env,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _kafkaConfig = kafkaConfig.Value;
        _env = env;
        _scopeFactory = scopeFactory;

        // Validate configuration
        if (_kafkaConfig.Topics == null || !_kafkaConfig.Topics.TryGetValue(TopicName, out string? topicAlias))
        {
             _logger.LogWarning("Topic alias '{TopicName}' not found in configuration. Using '{TopicName}' as actual topic name.", TopicName, TopicName);
             _topic = TopicName;
        }
        else
        {
             _topic = topicAlias;
        }

        if (_kafkaConfig.ConsumerGroups == null || !_kafkaConfig.ConsumerGroups.TryGetValue(GroupIdName, out string? groupAlias))
        {
             _logger.LogWarning("ConsumerGroup alias '{GroupIdName}' not found in configuration. Using '{GroupIdName}' as actual group ID.", GroupIdName, GroupIdName);
             _groupId = GroupIdName;
        }
        else
        {
             _groupId = groupAlias;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Kafka Consumer for {Topic} (Group: {GroupId})", _topic, _groupId);

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = _kafkaConfig.AutoOffsetReset?.Equals("Earliest", StringComparison.OrdinalIgnoreCase) == true ?
                AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            // Set the timeout to 11 minutes (in milliseconds)
            MaxPollIntervalMs = 660000
        };

        if (_kafkaConfig.IsMsk)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
            config.SocketTimeoutMs = 60000;
            config.SessionTimeoutMs = 30000;
        }

        await Task.Run(async () =>
        {
            var builder = new ConsumerBuilder<string, string>(config)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.Utf8)
                .SetErrorHandler((_, e) => _logger.LogError("Kafka error: {Reason} (Code: {Code})", e.Reason, e.Code));

            if (_kafkaConfig.IsMsk)
            {
                builder.SetOAuthBearerTokenRefreshHandler(OauthTokenRefreshCallback);
            }

            using var consumer = builder.Build();

            consumer.Subscribe(_topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    await ProcessNextMessage(consumer, stoppingToken);
                }
            }
            finally
            {
                consumer.Close();
                _logger.LogInformation("Stopped Kafka Consumer for {Topic}", _topic);
            }
        }, stoppingToken);
    }

    private async Task ProcessNextMessage(IConsumer<string, string> consumer, CancellationToken ct)
    {
        ConsumeResult<string, string>? cr = null;
        try
        {
            cr = consumer.Consume(ct);
            if (!IsValidConsumeResult(cr))
            {
                _logger.LogWarning("Bad Commit Message Received and Skipped to prevent poison pill");
                if (cr != null) 
                {
                    await SendToDlqAsync(cr, null, "Invalid ConsumeResult structure", ct);
                    consumer.Commit(cr);
                }
                return;
            }
            
            LogMessageReceived(cr!);

            TMessage? message;
            try 
            {
                message = JsonSerializer.Deserialize<TMessage>(cr!.Message.Value);
            }
            catch (JsonException ex)
            {
                 _logger.LogError(ex, "JSON deserialization failed via System.Text.Json");
                 // Skip poison message
                 await SendToDlqAsync(cr!, ex, "JSON Deserialization Failure", ct);
                 consumer.Commit(cr);
                 return;
            }

            if (message == null) 
            {
                 _logger.LogError("Deserialized message is null");
                 await SendToDlqAsync(cr!, null, "Deserialized payload is null", ct);
                 consumer.Commit(cr);
                 return;
            }

            try 
            {
                await ProcessMessageAsync(message, ct);
                consumer.Commit(cr);
            }
            catch (Exception processingEx)
            {
                _logger.LogError(processingEx, "Error during message processing. Routing to DLQ.");
                await SendToDlqAsync(cr!, processingEx, "Message Processing Exception", ct);
                // Commit to ensure we bypass this failing message upon restart!
                consumer.Commit(cr);
            }
        }
        catch (ConsumeException e)
        {
            _logger.LogError(e, "Consume error: {Reason}", e.Error.Reason);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Consumption was canceled");
            throw;
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Unhandled exception processing message");
            await Task.Delay(5000, ct); // Backoff on critical failures
        }
    }

    private void LogMessageReceived(ConsumeResult<string, string> cr)
    {
        if (_env.IsProduction())
        {
            _logger.LogInformation("RECEIVED message at {TopicPartitionOffset}", cr.TopicPartitionOffset);
        }
        else
        {
            // Be careful not to log PII in lower envs if possible, but user requested logging
            _logger.LogInformation("Consumed message: {Message} (Key: {Key}, Offset: {Offset})", 
                cr.Message.Value, cr.Message.Key, cr.TopicPartitionOffset);
        }
    }

    private bool IsValidConsumeResult(ConsumeResult<string, string>? cr)
    {
        if (cr == null)
        {
            return false;
        }
        if (cr.Message == null)
        {
             return false;
        }
        if (cr.Message.Key == null)
        {
            _logger.LogWarning("Received null key from Kafka message");
            return false;
        }
        if (string.IsNullOrEmpty(cr.Message.Value))
        {
            _logger.LogWarning("Received empty message from Kafka");
            return false;
        }
        
        // Basic JSON validation
        var strInput = cr.Message.Value.Trim();
        if (!(strInput.StartsWith("{") && strInput.EndsWith("}")) && 
            !(strInput.StartsWith("[") && strInput.EndsWith("]")))
        {
             _logger.LogWarning("Skipping non-JSON message");
             return false;
        }

        return true;
    }

    private void OauthTokenRefreshCallback(IClient client, string cfg)
    {
        try
        {
            AWSCredentials credentials;
            if (!string.IsNullOrEmpty(_kafkaConfig.AccessKey) && !string.IsNullOrEmpty(_kafkaConfig.SecretKey))
            {
                credentials = new BasicAWSCredentials(_kafkaConfig.AccessKey, _kafkaConfig.SecretKey);
            }
            else
            {
                 credentials = new EnvironmentVariablesAWSCredentials();
            }

            var (token, expiryMs) = _tokenGenerator.GenerateAuthTokenFromCredentialsProvider(
                () => credentials,
                Amazon.RegionEndpoint.GetBySystemName(_kafkaConfig.Region)
            ).Result;

            client.OAuthBearerSetToken(token, expiryMs, "PrincipalID");
        }
        catch (Exception ex)
        {
            client.OAuthBearerSetTokenFailure(ex.Message);
            _logger.LogError(ex, "[CRITICAL] Token Refresh Failed");
        }
    }
}
