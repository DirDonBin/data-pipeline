using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Core.Enums;

namespace Internal.DAL.Entities;

[Table("FileNodeConfigs", Schema = "public")]
public record FileNodeConfig
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    public required Guid DataNodeId { get; init; }
    
    [Required]
    [MaxLength(1000)]
    public required string FileUrl { get; init; } = string.Empty;
    
    [Required]
    public required FileType FileType { get; init; }
    
    [Required]
    public required DateTime CreatedAt { get; init; }
    
    [ForeignKey(nameof(DataNodeId))]
    public PipelineDataNode? DataNode { get; init; }
}
