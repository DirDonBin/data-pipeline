using System.ComponentModel.DataAnnotations;
using Core.Enums;

namespace Core.Models.Pipelines;

public record DatabaseNodeConfigDto
{
    [Required]
    public required DatabaseType DatabaseType { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Host { get; init; }
    
    [Required]
    [Range(1, 65535)]
    public required int Port { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Database { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Username { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Password { get; init; }
    
    [MaxLength(1000)]
    public string? AdditionalParams { get; init; }
}
