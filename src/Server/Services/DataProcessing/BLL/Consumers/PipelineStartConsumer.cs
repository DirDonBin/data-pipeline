using System.Text.Json;
using Core.Enums;
using Core.Models.Kafka;
using DataProcessing.BLL.Interfaces;
using DataProcessing.BLL.Producers;
using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataProcessing.BLL.Consumers;

internal class PipelineStartConsumer(
    IOptions<KafkaSettings> settings,
    ILogger<PipelineStartConsumer> logger,
    IProcessService processService,
    PipelineStatusProducer pipelineStatusProducer,
    AiRequestProducer aiRequestProducer)
    : KafkaConsumerBase<PipelineStartConsumer>(settings, logger)
{
    public override string Topic => "data_processing_start_generate";
    public override string GroupId => "data-processing-pipeline-group";

    protected override async Task HandleMessageAsync(string key, string message)
    {
        try
        {
            Logger.LogInformation("Получено сообщение о запуске пайплайна: {Key}", key);
            
            var pipelineMessage = JsonSerializer.Deserialize<PipelineStartMessage>(message, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
            });

            if (pipelineMessage == null)
            {
                Logger.LogError("Не удалось десериализовать сообщение пайплайна");
                return;
            }

            Logger.LogInformation("Обработка пайплайна {PipelineId} ({PipelineName})", 
                pipelineMessage.PipelineId, pipelineMessage.PipelineName);

            Logger.LogInformation("Отправка статуса 'Generating' для пайплайна {PipelineId}", pipelineMessage.PipelineId);
            await pipelineStatusProducer.SendAsync("",
                new PipelineStatusMessage()
                    { PipelineId = pipelineMessage.PipelineId, Status = PipelineStatus.Generating });
            
            Logger.LogInformation("Начало генерации схемы для пайплайна {PipelineId}", pipelineMessage.PipelineId);
            var request = await processService.GenerateScheme(pipelineMessage, CancellationToken.None);
            
            Logger.LogInformation("Отправка запроса в AI сервис для пайплайна {PipelineId}. RequestUid: {RequestUid}", 
                pipelineMessage.PipelineId, request.RequestUid);
            await aiRequestProducer.SendAsync(request.RequestUid.ToString(), request);
            
            Logger.LogInformation("Пайплайн {PipelineId} успешно обработан. Источников: {SourceCount}, Таргетов: {TargetCount}", 
                pipelineMessage.PipelineId, request.Sources.Count, request.Targets.Count);
            
            Logger.LogInformation("Отправка финального статуса 'Generating' для пайплайна {PipelineId}", pipelineMessage.PipelineId);
            await pipelineStatusProducer.SendAsync("",
                new PipelineStatusMessage()
                    { PipelineId = pipelineMessage.PipelineId, Status = PipelineStatus.Generating });
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Ошибка при обработке сообщения пайплайна: {Message}", message);
            throw;
        }
    }
}
