using System.Security.Claims;

namespace Internal.BLL.Interfaces;

public interface IJwtService
{
    string GenerateToken(Guid userId, string email);
    ClaimsPrincipal? ValidateToken(string token);
    DateTime GetTokenExpiration();
}
