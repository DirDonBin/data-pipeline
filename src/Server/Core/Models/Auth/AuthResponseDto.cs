namespace Core.Models.Auth;

public record AuthResponseDto
{
    public required string Token { get; init; }
    public required string TokenType { get; init; } = "Bearer";
    public required DateTime ExpiresAt { get; init; }
    public required UserDto User { get; init; }
}
