using Core.Models;
using Core.Models.Kafka;
using DataProcessing.BLL.Interfaces;
using DataProcessing.BLL.Producers;
using Microsoft.Extensions.Logging;

namespace DataProcessing.BLL.Services;

internal class KafkaService : IKafkaService
{
    private readonly ILogger<KafkaService> _logger;
    private readonly AiRequestProducer _aiRequestProducer;

    public KafkaService(
        ILogger<KafkaService> logger,
        AiRequestProducer aiRequestProducer)
    {
        _logger = logger;
        _aiRequestProducer = aiRequestProducer;
        _logger.LogInformation("KafkaService инициализирован");
    }

    public async Task<string> SendAiRequest(GenerateAiRequest request, CancellationToken cancellationToken = default)
    {
        var requestId = request.RequestUid;

        try
        {
            await _aiRequestProducer.SendAsync(requestId.ToString(), request, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка при отправке запроса в Kafka. RequestId: {RequestId}", requestId);
            throw;
        }

        return "";
    }
}
