using System;
using Microsoft.Extensions.DependencyInjection;
using SentinelKafka.Messaging;

namespace SentinelKafka.Extensions;

/// <summary>
/// Dependency Injection extension methods for integrating Sentinel Kafka services.
/// </summary>
public static class SentinelKafkaServiceCollectionExtensions
{
    /// <summary>
    /// Registers Sentinel Kafka resilient producers and consumers natively into the IServiceCollection.
    /// </summary>
    /// <param name="services">The IServiceCollection instance.</param>
    /// <param name="configureOptions">An action to configure the underlying <see cref="KafkaOptions"/>.</param>
    /// <returns>The original IServiceCollection instance for chaining.</returns>
    public static IServiceCollection AddSentinelKafka(this IServiceCollection services, Action<KafkaOptions> configureOptions)
    {
        services.AddOptions<KafkaOptions>()
            .Configure(configureOptions)
            .PostConfigure(options => options.Validate());
        
        // Register Core Services
        services.AddSingleton<KafkaPolicyFactory>();
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        services.AddSingleton<IResilientKafkaProducer, ResilientKafkaProducer>();
        
        // Register Open Generics for Schema Registry Producers natively
        services.AddTransient(typeof(IProtobufKafkaProducer<>), typeof(ProtobufKafkaProducer<>));

        return services;
    }
}
