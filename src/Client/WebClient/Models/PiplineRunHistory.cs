using WebClient.Enums;

namespace WebClient.Models
{
    public class PiplineRunHistory
    {
        public DateTime StartAt { get; set; }
        public DateTime EndAt { get; set; }
        public PipelineStatus Status { get; set; }
        public string? Description { get; set; } = "-";
    }
}
