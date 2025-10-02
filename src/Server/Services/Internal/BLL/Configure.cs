using System.Reflection;
using Internal.BLL.Interfaces;
using Internal.BLL.Producers;
using Internal.BLL.Services;
using Internal.DAL;
using Kafka.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Internal.BLL;

public static class Configure
{
    public static IServiceCollection AddBll(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddDal(configuration);
        
        services.AddKafkaInfrastructure(configuration);
        services.AddKafkaConsumers(Assembly.GetExecutingAssembly());
        
        services.AddSingleton<PipelineStartProducer>();
        
        services.AddSingleton<IJwtService, JwtService>();
        services.AddScoped<IAccountService, AccountService>();
        services.AddScoped<IPipelineService, PipelineService>();
        
        return services;
    }
}