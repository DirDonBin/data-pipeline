using Core.Models;
using Core.Models.Kafka;

namespace DataProcessing.BLL.Interfaces;

public interface IKafkaService
{
    Task<string> SendAiRequest(GenerateAiRequest request, CancellationToken cancellationToken = default);
}
