using WebClient.Enums;

namespace WebClient.Models
{
    public class PipelineDto
    {
        public string Name { get; set; } = "Название Pipeline";
        public PipelineStatus Status { get; set; } = PipelineStatus.Generated;

        public string? Dag { get; set; }
        public string? GenerationDescription { get; set; }

        public List<ConnectonSourceDto> ConnectonItems { get; set; } = new();
        public List<ConnectonTargetDto> ConnectonTargetItems { get; set; } = new();

        public List<PiplineRunHistory> RunHistory { get; set; } = new();
    }
}
