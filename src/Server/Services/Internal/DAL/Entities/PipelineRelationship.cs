using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Internal.DAL.Entities;

[Table("PipelineRelationships", Schema = "public")]
public record PipelineRelationship
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    public required Guid PipelineId { get; init; }
    
    [Required]
    public required Guid FromId { get; init; }
    
    [Required]
    public required Guid ToId { get; init; }
    
    [Required]
    public required int OrderIndex { get; init; }
    
    [Required]
    public required DateTime CreatedAt { get; init; }
    
    [ForeignKey(nameof(PipelineId))]
    public Pipeline? Pipeline { get; init; }
}
