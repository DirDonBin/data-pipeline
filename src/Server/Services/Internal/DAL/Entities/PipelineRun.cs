using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Internal.DAL.Entities;

[Table("PipelineRuns", Schema = "public")]
public record PipelineRun
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    public required Guid PipelineId { get; init; }
    
    [Required]
    public required Guid StartedByUserId { get; init; }
    
    [Required]
    public required DateTime StartedAt { get; init; }
    
    public DateTime? CompletedAt { get; init; }
    
    public int? DurationSeconds { get; init; }
    
    public string? ErrorMessage { get; init; }
    
    [ForeignKey(nameof(PipelineId))]
    public Pipeline? Pipeline { get; init; }
    
    [ForeignKey(nameof(StartedByUserId))]
    public User? StartedByUser { get; init; }
}
