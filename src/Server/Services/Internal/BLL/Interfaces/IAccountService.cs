using Core.Models.Auth;

namespace Internal.BLL.Interfaces;

public interface IAccountService
{
    Task<AuthResponseDto> RegisterAsync(RegisterDto registerDto);
    Task<AuthResponseDto> LoginAsync(LoginDto loginDto);
}