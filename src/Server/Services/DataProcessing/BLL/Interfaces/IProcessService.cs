using Core.Models;
using Core.Models.Kafka;
using Microsoft.AspNetCore.Http;

namespace DataProcessing.BLL.Interfaces;

public interface IProcessService
{
    Task GenerateScheme(List<IFormFile> files, CancellationToken cancellationToken = default);
    Task<GenerateAiRequest> GenerateScheme(PipelineStartMessage pipelineMessage, CancellationToken cancellationToken = default);
}