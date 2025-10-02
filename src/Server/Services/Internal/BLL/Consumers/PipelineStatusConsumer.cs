using System.Text.Json;
using Core.Models;
using Core.Models.Kafka;
using Internal.BLL.Interfaces;
using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Internal.BLL.Consumers;

internal class PipelineStatusConsumer : KafkaConsumerBase<PipelineStatusConsumer>
{
    private readonly IServiceProvider _serviceProvider;
    
    public override string Topic => "pipeline_status_update";
    public override string GroupId => "internal-pipeline-status-group";

    public PipelineStatusConsumer(
        IOptions<KafkaSettings> settings,
        ILogger<PipelineStatusConsumer> logger,
        IServiceProvider serviceProvider) 
        : base(settings, logger)
    {
        _serviceProvider = serviceProvider;
    }

    protected override async Task HandleMessageAsync(string key, string message)
    {
        try
        {
            var statusMessage = JsonSerializer.Deserialize<PipelineStatusMessage>(message, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true
            });

            if (statusMessage == null)
            {
                Logger.LogWarning("Не удалось десериализовать сообщение о статусе пайплайна. Key: {Key}", key);
                return;
            }

            Logger.LogInformation("Обновление статуса пайплайна {PipelineId} на {Status}", 
                statusMessage.PipelineId, statusMessage.Status);

            using var scope = _serviceProvider.CreateScope();
            var pipelineService = scope.ServiceProvider.GetRequiredService<IPipelineService>();

            await pipelineService.ChangePipelineStatus(statusMessage.PipelineId, statusMessage.Status);

            Logger.LogInformation("Статус пайплайна {PipelineId} успешно обновлен на {Status}", 
                statusMessage.PipelineId, statusMessage.Status);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Ошибка при обработке сообщения о статусе пайплайна. Key: {Key}", key);
        }
    }
}
