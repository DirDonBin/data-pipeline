using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Internal.DAL.Entities;

[Table("Users", Schema = "public")]
public record User
{
    [Key]
    public required Guid Id { get; init; }
    
    [Required]
    [MaxLength(100)]
    public required string FirstName { get; init; } = string.Empty;
    
    [Required]
    [MaxLength(100)]
    public required string LastName { get; init; } = string.Empty;
    
    [Required]
    [MaxLength(255)]
    public required string Email { get; init; } = string.Empty;
    
    [Required]
    [MaxLength(500)]
    public required string PasswordHash { get; init; } = string.Empty;
    
    [Required]
    [MaxLength(500)]
    public required string PasswordSalt { get; init; } = string.Empty;
    
    [Required]
    public required DateTime CreatedAt { get; init; }
}