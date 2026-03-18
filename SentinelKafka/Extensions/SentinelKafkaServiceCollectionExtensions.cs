using System;
using Microsoft.Extensions.DependencyInjection;
using SentinelKafka.Messaging;

namespace SentinelKafka.Extensions;

public static class SentinelKafkaServiceCollectionExtensions
{
    public static IServiceCollection AddSentinelKafka(this IServiceCollection services, Action<KafkaOptions> configureOptions)
    {
        services.AddOptions<KafkaOptions>()
            .Configure(configureOptions)
            .PostConfigure(options => options.Validate());
        
        services.AddSingleton<KafkaPolicyFactory>();
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        services.AddSingleton<IResilientKafkaProducer, ResilientKafkaProducer>();
        
        return services;
    }
}
