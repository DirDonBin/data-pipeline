using Core.Enums;

namespace Core.Models.Kafka;

public record PipelineStatusMessage
{
    public required Guid PipelineId { get; init; }
    public required PipelineStatus Status { get; init; }
}
