using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace SentinelKafka.Messaging;

/// <summary>
/// Configuration options for Sentinel Kafka Producers and Consumers.
/// </summary>
public class KafkaOptions
{
    /// <summary>
    /// A comma-separated list of host:port pairs indicating the Kafka cluster.
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Controls the number of acknowledgments the producer requires the leader to have received before considering a request complete.
    /// Defaults to Acks.All for highest durability.
    /// </summary>
    public Acks Ack { get; set; } = Acks.All;

    /// <summary>
    /// Delay in milliseconds to wait for additional messages in order to batch them together.
    /// Default is 5ms.
    /// </summary>
    public double LingerMs { get; set; } = 5;

    /// <summary>
    /// The maximum number of times to retry sending a failing message.
    /// Default is 3 retries.
    /// </summary>
    public int MessageSendMaxRetries { get; set; } = 3;

    /// <summary>
    /// Determines what to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server.
    /// Examples: "Earliest", "Latest".
    /// </summary>
    public string? AutoOffsetReset { get; set; }

    /// <summary>
    /// A dictionary mapping logical topic aliases to actual Kafka physical topic names.
    /// </summary>
    public Dictionary<string, string>? Topics { get; set; }

    /// <summary>
    /// A dictionary mapping logical consumer group aliases to actual Kafka physical consumer group IDs.
    /// </summary>
    public Dictionary<string, string>? ConsumerGroups { get; set; }

    /// <summary>
    /// The HTTP URL of the Confluent Schema Registry (Required for strictly-typed Protobuf/Avro serializers).
    /// </summary>
    public string? SchemaRegistryUrl { get; set; }

    // AWS MSK Settings

    /// <summary>
    /// Set to true to enable AWS MSK IAM authentication provisioning.
    /// </summary>
    public bool IsMsk { get; set; } = false;

    /// <summary>
    /// The AWS Access Key profile required when IsMsk is true.
    /// </summary>
    public string? AccessKey { get; set; }

    /// <summary>
    /// The AWS Secret Key profile required when IsMsk is true.
    /// </summary>
    public string? SecretKey { get; set; }

    /// <summary>
    /// The AWS Region of the MSK cluster (e.g., "us-east-1"). Defaults to "us-east-1".
    /// </summary>
    public string Region { get; set; } = "us-east-1";

    /// <summary>
    /// Performs runtime validations on the configuration structure. 
    /// Ensures MSK parameters are hydrated securely if IsMsk is set to true.
    /// </summary>
    public void Validate()
    {
        if (IsMsk)
        {
            if (string.IsNullOrWhiteSpace(AccessKey)) throw new InvalidOperationException("AccessKey is required when IsMsk is true.");
            if (string.IsNullOrWhiteSpace(SecretKey)) throw new InvalidOperationException("SecretKey is required when IsMsk is true.");
            if (string.IsNullOrWhiteSpace(Region)) throw new InvalidOperationException("Region is required when IsMsk is true.");
        }
    }
}
