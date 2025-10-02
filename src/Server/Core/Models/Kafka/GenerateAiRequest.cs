using Core.Models.Files.Sources;

namespace Core.Models.Kafka;

public record GenerateAiRequest
{
    public required Guid RequestUid { get; set; }
    public required Guid PipelineUid { get; set; }
    public DateTime Timestamp => DateTime.UtcNow;
    public required List<SourceDto> Sources { get; set; }
    public required List<SourceDto> Targets { get; set; }
}
