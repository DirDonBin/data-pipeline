using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Core.Enums;

namespace Internal.DAL.Entities;

[Table("Pipelines", Schema = "public")]
public record Pipeline
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Name { get; init; } = string.Empty;
    
    [MaxLength(1000)]
    public string? Description { get; init; }
    
    public string? DagFile { get; init; }
    
    [Required]
    public required PipelineStatus Status { get; init; }
    
    [Required]
    public required Guid CreatedUserId { get; init; }
    
    [Required]
    public required DateTime CreatedAt { get; init; }
    
    public DateTime? UpdatedAt { get; init; }
    
    [Required]
    public required bool IsActive { get; init; }
    
    [ForeignKey(nameof(CreatedUserId))]
    public User? CreatedUser { get; init; }
    
    public ICollection<PipelineDataNode>? DataNodes { get; init; }
    public ICollection<PipelineFunction>? Functions { get; init; }
    public ICollection<PipelineRelationship>? Relationships { get; init; }
}
