using System.ComponentModel.DataAnnotations;
using Core.Enums;

namespace Core.Models.Pipelines;

public record DataNodeDto
{
    [Required]
    [MaxLength(255)]
    public required string Name { get; init; }
    
    [Required]
    public required DataNodeType DataNodeType { get; init; }
    
    public FileNodeConfigDto? FileConfig { get; init; }
    
    public DatabaseNodeConfigDto? DatabaseConfig { get; init; }
}
