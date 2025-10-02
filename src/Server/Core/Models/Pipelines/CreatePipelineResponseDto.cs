namespace Core.Models.Pipelines;

public record CreatePipelineResponseDto
{
    public required Guid PipelineId { get; init; }
}
