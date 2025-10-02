using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Core.Enums;

namespace Internal.DAL.Entities;

[Table("DatabaseNodeConfigs", Schema = "public")]
public record DatabaseNodeConfig
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    public required Guid DataNodeId { get; init; }
    
    [Required]
    public required DatabaseType DatabaseType { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Host { get; init; } = string.Empty;
    
    [Required]
    public required int Port { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Database { get; init; } = string.Empty;
    
    [Required]
    [MaxLength(255)]
    public required string Username { get; init; } = string.Empty;
    
    [Required]
    [MaxLength(255)]
    public required string Password { get; init; } = string.Empty;
    
    [MaxLength(1000)]
    public string? AdditionalParams { get; init; }
    
    [Required]
    public required DateTime CreatedAt { get; init; }
    
    [ForeignKey(nameof(DataNodeId))]
    public PipelineDataNode? DataNode { get; init; }
}
