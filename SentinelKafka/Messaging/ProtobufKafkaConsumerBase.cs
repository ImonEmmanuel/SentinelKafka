using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using AWS.MSK.Auth;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SentinelKafka.Messaging;

public abstract class ProtobufKafkaConsumerBase<TMessage> : BackgroundService where TMessage : class, IMessage<TMessage>, new()
{
    protected readonly ILogger _logger;
    protected readonly AWSMSKAuthTokenGenerator _tokenGenerator = new AWSMSKAuthTokenGenerator();

    protected readonly KafkaOptions _kafkaConfig;
    protected readonly IHostEnvironment _env;
    protected readonly IServiceScopeFactory _scopeFactory;

    protected readonly string _topic;
    protected readonly string _groupId;
    
    private ISchemaRegistryClient? _schemaRegistryClient;

    protected abstract string TopicName { get; }
    protected abstract string GroupIdName { get; }
    
    protected abstract Task ProcessMessageAsync(TMessage message, CancellationToken ct);

    protected virtual Task SendToDlqAsync(ConsumeResult<string, TMessage> consumeResult, Exception? ex, string reason, CancellationToken ct)
    {
        _logger.LogWarning("Protobuf message routed to DLQ path manually without explicit override hook handled. Reason: {Reason}", reason);
        return Task.CompletedTask;
    }

    protected ProtobufKafkaConsumerBase(
        ILogger logger,
        IOptions<KafkaOptions> kafkaConfig,
        IHostEnvironment env,
        IServiceScopeFactory scopeFactory)
    {
        _logger = logger;
        _kafkaConfig = kafkaConfig.Value;
        _env = env;
        _scopeFactory = scopeFactory;

        if (_kafkaConfig.Topics == null || !_kafkaConfig.Topics.TryGetValue(TopicName, out string? topicAlias))
             _topic = TopicName;
        else
             _topic = topicAlias;

        if (_kafkaConfig.ConsumerGroups == null || !_kafkaConfig.ConsumerGroups.TryGetValue(GroupIdName, out string? groupAlias))
             _groupId = GroupIdName;
        else
             _groupId = groupAlias;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Starting Protobuf Schema Registry Consumer for {Topic} (Group: {GroupId})", _topic, _groupId);

        var config = new ConsumerConfig
        {
            BootstrapServers = _kafkaConfig.BootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = _kafkaConfig.AutoOffsetReset?.Equals("Earliest", StringComparison.OrdinalIgnoreCase) == true ?
                AutoOffsetReset.Earliest : AutoOffsetReset.Latest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            // Timeout manually escalated safely
            MaxPollIntervalMs = 660000
        };

        if (_kafkaConfig.IsMsk)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
            config.SocketTimeoutMs = 60000;
            config.SessionTimeoutMs = 30000;
        }

        if (string.IsNullOrEmpty(_kafkaConfig.SchemaRegistryUrl))
        {
            throw new InvalidOperationException("SchemaRegistryUrl must be rigorously configured for Protobuf consumer routing via Sentinel.");
        }

        var schemaConfig = new SchemaRegistryConfig { Url = _kafkaConfig.SchemaRegistryUrl };
        _schemaRegistryClient = new CachedSchemaRegistryClient(schemaConfig);

        await Task.Run(async () =>
        {
            var builder = new ConsumerBuilder<string, TMessage>(config)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(new ProtobufDeserializer<TMessage>(_schemaRegistryClient).AsSyncOverAsync())
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
                _schemaRegistryClient?.Dispose();
                _logger.LogInformation("Stopped Protobuf Kafka Consumer for {Topic}", _topic);
            }
        }, stoppingToken);
    }

    private async Task ProcessNextMessage(IConsumer<string, TMessage> consumer, CancellationToken ct)
    {
        ConsumeResult<string, TMessage>? cr = null;
        try
        {
            cr = consumer.Consume(ct);
            if (cr?.Message == null) return;
            
            // Rehydrate OpenTelemetry Activity Context intelligently
            var parentId = string.Empty;
            var traceState = string.Empty;
            if (cr.Message.Headers != null)
            {
                if (cr.Message.Headers.TryGetLastBytes("traceparent", out var traceIdBytes))
                    parentId = Encoding.UTF8.GetString(traceIdBytes);
                if (cr.Message.Headers.TryGetLastBytes("tracestate", out var traceStateBytes))
                    traceState = Encoding.UTF8.GetString(traceStateBytes);
            }

            using var activity = new Activity("ProtobufKafkaConsumerBase.ProcessMessage");
            if (!string.IsNullOrEmpty(parentId))
            {
                activity.SetParentId(parentId);
                if (!string.IsNullOrEmpty(traceState))
                    activity.TraceStateString = traceState;
            }
            activity.Start();

            if (cr.Message.Value == null) 
            {
                 _logger.LogError("Deserialized protobuf message evaluates to null");
                 await SendToDlqAsync(cr, null, "Deserialized protobuf payload yields null", ct);
                 consumer.Commit(cr);
                 return;
            }

            try 
            {
                await ProcessMessageAsync(cr.Message.Value, ct);
                consumer.Commit(cr);
            }
            catch (Exception processingEx)
            {
                _logger.LogError(processingEx, "Error during Protobuf message processing. Routing natively to DLQ handler.");
                await SendToDlqAsync(cr, processingEx, "Message Processing Exception", ct);
                consumer.Commit(cr);
            }
        }
        catch (ConsumeException e)
        {
            var crExc = e.ConsumerRecord as ConsumeResult<string, TMessage>;
            _logger.LogError(e, "Protobuf Consume or Binary Evaluation Validation error: {Reason}", e.Error.Reason);
            // Handling Protobuf format deserialization explicitly decoupled into ConsumeException events seamlessly routed to DLQ
            if (crExc != null)
            {
                await SendToDlqAsync(crExc, e, "Protobuf schema validation rejection or Consume parsing pipeline Error", ct);
                consumer.Commit(crExc);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Background MSK consumption thread was gracefully canceled");
            throw;
        }
        catch (Exception e)
        {
            _logger.LogCritical(e, "Unhandled exception processing underlying system execution thread inside Sentinel protobuf wrapper");
            await Task.Delay(5000, ct); 
        }
    }

    private void OauthTokenRefreshCallback(IClient client, string cfg)
    {
        try
        {
            AWSCredentials credentials = !string.IsNullOrEmpty(_kafkaConfig.AccessKey) && !string.IsNullOrEmpty(_kafkaConfig.SecretKey) 
                ? new BasicAWSCredentials(_kafkaConfig.AccessKey, _kafkaConfig.SecretKey) 
                : new EnvironmentVariablesAWSCredentials();

            var (token, expiryMs) = _tokenGenerator.GenerateAuthTokenFromCredentialsProvider(
                () => credentials,
                Amazon.RegionEndpoint.GetBySystemName(_kafkaConfig.Region)
            ).Result;

            client.OAuthBearerSetToken(token, expiryMs, "PrincipalID");
        }
        catch (Exception ex)
        {
            client.OAuthBearerSetTokenFailure(ex.Message);
            _logger.LogError(ex, "[CRITICAL] Token Refresh Execution Failed inside the Protobuf Consumer worker thread context");
        }
    }
}
