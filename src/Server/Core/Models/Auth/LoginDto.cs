using System.ComponentModel.DataAnnotations;

namespace Core.Models.Auth;

public record LoginDto
{
    [Required(ErrorMessage = "Email обязателен")]
    [EmailAddress(ErrorMessage = "Некорректный формат email")]
    public required string Email { get; init; }
    
    [Required(ErrorMessage = "Пароль обязателен")]
    public required string Password { get; init; }
}
