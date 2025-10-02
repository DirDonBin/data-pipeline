using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataProcessing.BLL.Consumers;

internal class AiResponseConsumer : KafkaConsumerBase<AiResponseConsumer>
{
    public override string Topic => "ai_generate_response";
    public override string GroupId => "data-processing-group";

    public AiResponseConsumer(
        IOptions<KafkaSettings> settings,
        ILogger<AiResponseConsumer> logger) 
        : base(settings, logger)
    {
    }

    protected override Task HandleMessageAsync(string key, string message)
    {
        if (Guid.TryParse(key, out var requestId))
        {
            Logger.LogInformation("Ответ обработан для RequestId: {RequestId}", requestId);
        }
        else
        {
            Logger.LogWarning("Не удалось распарсить RequestId из ключа: {Key}", key);
        }

        return Task.CompletedTask;
    }
}
