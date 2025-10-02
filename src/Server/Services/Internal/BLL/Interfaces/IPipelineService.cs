using Core.Enums;
using Core.Models.Pipelines;

namespace Internal.BLL.Interfaces;

public interface IPipelineService
{
    Task<Guid> CreatePipelineAsync(CreatePipelineDto dto, Guid userId, CancellationToken cancellationToken = default);
    Task ChangePipelineStatus(Guid pipelineId, PipelineStatus status);
}
