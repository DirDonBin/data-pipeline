using System.ComponentModel.DataAnnotations;

namespace Core.Models.Auth;

public record RegisterDto
{
    [Required(ErrorMessage = "Имя обязательно")]
    [MaxLength(100, ErrorMessage = "Имя не должно превышать 100 символов")]
    public required string FirstName { get; init; }
    
    [Required(ErrorMessage = "Фамилия обязательна")]
    [MaxLength(100, ErrorMessage = "Фамилия не должна превышать 100 символов")]
    public required string LastName { get; init; }
    
    [Required(ErrorMessage = "Email обязателен")]
    [EmailAddress(ErrorMessage = "Некорректный формат email")]
    [MaxLength(255, ErrorMessage = "Email не должен превышать 255 символов")]
    public required string Email { get; init; }
    
    [Required(ErrorMessage = "Пароль обязателен")]
    [MinLength(6, ErrorMessage = "Пароль должен содержать минимум 6 символов")]
    [MaxLength(100, ErrorMessage = "Пароль не должен превышать 100 символов")]
    public required string Password { get; init; }
}
