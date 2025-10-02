using Core.Models.Pipelines;

namespace Core.Models.Kafka;

public record PipelineStartMessage
{
    public required Guid RequestUid { get; set; }
    public required Guid PipelineId { get; init; }
    public required string PipelineName { get; init; }
    public required List<DataNodeDto> Sources { get; init; }
    public required List<DataNodeDto> Targets { get; init; }
}
