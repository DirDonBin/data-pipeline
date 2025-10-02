using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Internal.DAL.Entities;

[Table("PipelineFunctions", Schema = "public")]
public record PipelineFunction
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    public required Guid PipelineId { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Name { get; init; } = string.Empty;
    public required string FunctionName { get; init; } = string.Empty;
    
    [Required]
    public required string Function { get; init; } = string.Empty;
    
    [Required]
    public required DateTime CreatedAt { get; init; }
    
    [ForeignKey(nameof(PipelineId))]
    public Pipeline? Pipeline { get; init; }
}
