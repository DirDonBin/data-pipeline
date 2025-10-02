using System.Reflection;
using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.HostedServices;
using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Services;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Kafka.Infrastructure;

public static class Configure
{
    public static IServiceCollection AddKafkaInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration,
        string configSectionName = "Kafka")
    {
        services.Configure<KafkaSettings>(configuration.GetSection(configSectionName));
        services.AddSingleton<IKafkaProducer, KafkaProducer>();
        
        return services;
    }

    public static IServiceCollection AddKafkaConsumers(
        this IServiceCollection services,
        Assembly assembly)
    {
        var consumerTypes = assembly.GetTypes()
            .Where(t => t.IsClass && !t.IsAbstract && typeof(IMessageConsumer).IsAssignableFrom(t))
            .ToList();

        foreach (var consumerType in consumerTypes)
        {
            services.AddSingleton(typeof(IMessageConsumer), consumerType);
            services.AddHostedService(sp => new ConsumerHostedService(
                sp.GetRequiredService<IMessageConsumer>()));
        }

        return services;
    }
}
