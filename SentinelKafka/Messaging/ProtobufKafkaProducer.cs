using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Amazon.Runtime;
using AWS.MSK.Auth;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SentinelKafka.Messaging;

/// <summary>
/// A resilient producer configured natively to enforce Confluent Schema Registry Protobuf serialization over the AWS MSK transports.
/// </summary>
public interface IProtobufKafkaProducer<TValue> where TValue : class, IMessage<TValue>, new()
{
    Task ProduceAsync(string topic, string key, TValue message);
}

public class ProtobufKafkaProducer<TValue> : IProtobufKafkaProducer<TValue>, IDisposable where TValue : class, IMessage<TValue>, new()
{
    private readonly IProducer<string, TValue> _producer;
    private readonly ISchemaRegistryClient _schemaRegistry;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<ProtobufKafkaProducer<TValue>> _logger;
    private readonly AWSMSKAuthTokenGenerator _tokenGenerator = new AWSMSKAuthTokenGenerator();

    public ProtobufKafkaProducer(
        IOptions<KafkaOptions> options,
        ILogger<ProtobufKafkaProducer<TValue>> logger)
    {
        _kafkaOptions = options.Value;
        _logger = logger;

        if (string.IsNullOrEmpty(_kafkaOptions.SchemaRegistryUrl))
        {
            throw new InvalidOperationException("SchemaRegistryUrl must be explicitly configured to use the ProtobufKafkaProducer.");
        }

        var schemaConfig = new SchemaRegistryConfig
        {
            Url = _kafkaOptions.SchemaRegistryUrl
        };
        _schemaRegistry = new CachedSchemaRegistryClient(schemaConfig);

        var config = new ProducerConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers,
            Acks = _kafkaOptions.Ack,
            LingerMs = _kafkaOptions.LingerMs,
            EnableIdempotence = true,
            MessageSendMaxRetries = _kafkaOptions.MessageSendMaxRetries
        };

        if (_kafkaOptions.IsMsk)
        {
            config.SecurityProtocol = SecurityProtocol.SaslSsl;
            config.SaslMechanism = SaslMechanism.OAuthBearer;
            config.SocketTimeoutMs = 60000;
            config.MessageTimeoutMs = 300000;
        }

        var builder = new ProducerBuilder<string, TValue>(config)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(new ProtobufSerializer<TValue>(_schemaRegistry).AsSyncOverAsync());

        if (_kafkaOptions.IsMsk)
        {
            builder.SetOAuthBearerTokenRefreshHandler(OauthTokenRefreshCallback);
        }

        _producer = builder.Build();
    }

    public async Task ProduceAsync(string topic, string key, TValue message)
    {
        var actualTopic = GetTopic(topic);
        if (actualTopic == null) return;

        var headers = new Headers();
        if (Activity.Current != null)
        {
            headers.Add("traceparent", Encoding.UTF8.GetBytes(Activity.Current.Id!));
            if (!string.IsNullOrEmpty(Activity.Current.TraceStateString))
            {
                headers.Add("tracestate", Encoding.UTF8.GetBytes(Activity.Current.TraceStateString));
            }
        }

        try
        {
            var result = await _producer.ProduceAsync(
                actualTopic,
                new Message<string, TValue> { Key = key, Value = message, Headers = headers });

            _logger.LogInformation(
                "Produced Protobuf message to {Topic} [Partition: {Partition}, Offset: {Offset}] Key={Key}",
                actualTopic, result.Partition, result.Offset, key);
        }
        catch (ProduceException<string, TValue> ex)
        {
            _logger.LogError(ex, "Error producing strictly-typed Protobuf message to {Topic}. Key={Key}, Reason={Reason}", topic, key, ex.Error.Reason);
            throw;
        }
    }

    private string GetTopic(string topic)
    {
        if (_kafkaOptions.Topics != null && _kafkaOptions.Topics.TryGetValue(topic, out var kafkaTopic))
        {
            return kafkaTopic;
        }
        return topic;
    }

    private void OauthTokenRefreshCallback(IClient client, string cfg)
    {
        try
        {
            AWSCredentials credentials = !string.IsNullOrEmpty(_kafkaOptions.AccessKey) && !string.IsNullOrEmpty(_kafkaOptions.SecretKey) 
                ? new BasicAWSCredentials(_kafkaOptions.AccessKey, _kafkaOptions.SecretKey) 
                : new EnvironmentVariablesAWSCredentials();

            var (token, expiryMs) = _tokenGenerator.GenerateAuthTokenFromCredentialsProvider(
                () => credentials,
                Amazon.RegionEndpoint.GetBySystemName(_kafkaOptions.Region)
            ).Result;

            client.OAuthBearerSetToken(token, expiryMs, "PrincipalID");
        }
        catch (Exception ex)
        {
            client.OAuthBearerSetTokenFailure(ex.Message);
            _logger.LogError(ex, "[CRITICAL] Token Refresh Failed in ProtobufProducer");
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        _schemaRegistry.Dispose();
        GC.SuppressFinalize(this);
    }
}
