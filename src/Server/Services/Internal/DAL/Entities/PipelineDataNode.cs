using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Core.Enums;

namespace Internal.DAL.Entities;

[Table("PipelineDataNodes", Schema = "public")]
public record PipelineDataNode
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    public required Guid PipelineId { get; init; }
    
    [Required]
    [MaxLength(255)]
    public required string Name { get; init; } = string.Empty;
    
    [Required]
    public required NodeType NodeType { get; init; }
    
    [Required]
    public required DataNodeType DataNodeType { get; init; }
    
    [Required]
    public required DateTime CreatedAt { get; init; }
    
    [ForeignKey(nameof(PipelineId))]
    public Pipeline? Pipeline { get; init; }
    
    public FileNodeConfig? FileConfig { get; init; }
    public DatabaseNodeConfig? DatabaseConfig { get; init; }
}
