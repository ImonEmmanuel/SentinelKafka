using System;
using System.Text.Json;
using System.Threading.Tasks;
using Amazon.Runtime;
using AWS.MSK.Auth;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SentinelKafka.Messaging;

public class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly KafkaOptions _kafkaOptions;
    private readonly ILogger<KafkaProducer> _logger;
    private readonly AWSMSKAuthTokenGenerator _tokenGenerator = new AWSMSKAuthTokenGenerator();

    public KafkaProducer(
            IOptions<KafkaOptions> options,
            ILogger<KafkaProducer> logger)
    {
        _kafkaOptions = options.Value;
        _logger = logger;

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

        var builder = new ProducerBuilder<string, string>(config)
            .SetKeySerializer(Serializers.Utf8)
            .SetValueSerializer(Serializers.Utf8);

        if (_kafkaOptions.IsMsk)
        {
            builder.SetOAuthBearerTokenRefreshHandler(OauthTokenRefreshCallback);
        }

        _producer = builder.Build();
    }

    public async Task ProduceAsync(string topic, string value)
    {
        var key = Guid.NewGuid().ToString();
        await ProduceInternalAsync(topic, key, value);
    }

    public async Task ProduceAsync<T>(string topic, T payload, string? key = null)
    {
        var serialized = JsonSerializer.Serialize(payload);
        await ProduceInternalAsync(topic, key ?? Guid.NewGuid().ToString(), serialized);
    }

    private async Task ProduceInternalAsync(string topic, string key, string value)
    {
        try
        {
            var actualTopic = GetTopic(topic);
        
            if (actualTopic == null) return;

            var result = await _producer.ProduceAsync(
                actualTopic,
                new Message<string, string> { Key = key, Value = value });

            _logger.LogInformation(
                "Produced message to {Topic} [Partition: {Partition}, Offset: {Offset}] Key={Key} Size={Size} bytes",
                actualTopic, result.Partition, result.Offset, key, value.Length
            );
        }
        catch (ProduceException<string, string> ex)
        {
            _logger.LogError(ex,
                "Error producing message to {Topic}. Key={Key}, Reason={Reason}",
                topic, key, ex.Error.Reason);
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
            AWSCredentials credentials = new BasicAWSCredentials(_kafkaOptions.AccessKey, _kafkaOptions.SecretKey);
            var (token, expiryMs) = _tokenGenerator.GenerateAuthTokenFromCredentialsProvider(
                () => credentials,
                Amazon.RegionEndpoint.GetBySystemName(_kafkaOptions.Region)
            ).Result;

            client.OAuthBearerSetToken(token, expiryMs, "PrincipalID");

            _logger.LogDebug("MSK OAuth token refreshed successfully for producer");
        }
        catch (Exception ex)
        {
            client.OAuthBearerSetTokenFailure(ex.Message);
            Console.Error.WriteLine($"Producer Token Error: {ex.Message}");
        }
    }

    public void Dispose()
    {
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        GC.SuppressFinalize(this);
    }
}
