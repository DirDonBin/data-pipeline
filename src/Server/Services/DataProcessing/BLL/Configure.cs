using System.Reflection;
using DataProcessing.BLL.Interfaces;
using DataProcessing.BLL.Producers;
using DataProcessing.BLL.Services;
using Kafka.Infrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace DataProcessing.BLL;

public static class Configure
{
    public static IServiceCollection AddBll(this IServiceCollection services, IConfiguration configuration)
    {
        services.AddKafkaInfrastructure(configuration);
        services.AddKafkaConsumers(Assembly.GetExecutingAssembly());
        
        services.AddSingleton<AiRequestProducer>();
        services.AddSingleton<PipelineStatusProducer>();
        
        services.AddHttpClient<IHadoopService, HadoopService>()
            .ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler
            {
                AllowAutoRedirect = false
            });
        services.AddSingleton<IKafkaService, KafkaService>();
        services.AddScoped<IProcessService, ProcessService>();
        services.AddScoped<IFileService, FileService>();
        services.AddScoped<IFileUploadService, FileUploadService>();
        services.AddScoped<IDatabaseService, DatabaseService>();

        return services;
    }
}