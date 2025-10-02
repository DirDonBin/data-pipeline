using System.ComponentModel.DataAnnotations;

namespace Core.Models.Pipelines;

public record CreatePipelineDto
{
    [Required]
    [MaxLength(255)]
    public required string Name { get; init; }
    
    [MaxLength(1000)]
    public string? Description { get; init; }
    
    [Required]
    [MinLength(1)]
    public required List<DataNodeDto> Sources { get; init; }
    
    [Required]
    [MinLength(1)]
    public required List<DataNodeDto> Targets { get; init; }
}
