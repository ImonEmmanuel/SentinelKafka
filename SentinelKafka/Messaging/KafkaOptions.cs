using System;
using System.Collections.Generic;
using Confluent.Kafka;

namespace SentinelKafka.Messaging;

public class KafkaOptions
{
    public string BootstrapServers { get; set; } = string.Empty;
    public Acks Ack { get; set; } = Acks.All;
    public double LingerMs { get; set; } = 5;
    public int MessageSendMaxRetries { get; set; } = 3;

    // AWS MSK Settings
    public bool IsMsk { get; set; } = false;
    public string? AccessKey { get; set; }
    public string? SecretKey { get; set; }
    public string Region { get; set; } = "us-east-1";

    public string? AutoOffsetReset { get; set; }
    public Dictionary<string, string>? Topics { get; set; }
    public Dictionary<string, string>? ConsumerGroups { get; set; }

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
