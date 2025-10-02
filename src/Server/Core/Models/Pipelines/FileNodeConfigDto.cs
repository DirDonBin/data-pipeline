using System.ComponentModel.DataAnnotations;
using Core.Enums;

namespace Core.Models.Pipelines;

public record FileNodeConfigDto
{
    [Required]
    [MaxLength(1000)]
    public required string FileUrl { get; init; }
    
    [Required]
    public required FileType FileType { get; init; }
}
