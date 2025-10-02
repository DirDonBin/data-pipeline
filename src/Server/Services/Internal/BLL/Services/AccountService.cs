using System.Security.Cryptography;
using Core.Models.Auth;
using Internal.BLL.Interfaces;
using Internal.DAL;
using Internal.DAL.Entities;
using Microsoft.EntityFrameworkCore;

namespace Internal.BLL.Services;

internal class AccountService(
    PipelineDbContext context,
    IJwtService jwtService) : IAccountService
{
    public async Task<AuthResponseDto> RegisterAsync(RegisterDto registerDto)
    {
        var isExistingUser = await context.Users
            .Where(u => u.Email == registerDto.Email.ToLowerInvariant())
            .AnyAsync();

        if (isExistingUser)
            throw new InvalidOperationException("Пользователь с таким email уже существует");

        var (passwordHash, passwordSalt) = HashPassword(registerDto.Password);

        var user = new User
        {
            Id = Guid.CreateVersion7(),
            FirstName = registerDto.FirstName,
            LastName = registerDto.LastName,
            Email = registerDto.Email.ToLowerInvariant(),
            PasswordHash = passwordHash,
            PasswordSalt = passwordSalt,
            CreatedAt = DateTime.UtcNow
        };

        context.Users.Add(user);

        await context.SaveChangesAsync();
        
        var token = jwtService.GenerateToken(user.Id, user.Email);
        var expiresAt = jwtService.GetTokenExpiration();

        return new AuthResponseDto
        {
            Token = token,
            TokenType = "Bearer",
            ExpiresAt = expiresAt,
            User = new UserDto
            {
                Id = user.Id,
                FirstName = user.FirstName,
                LastName = user.LastName,
                Email = user.Email
            }
        };
    }

    public async Task<AuthResponseDto> LoginAsync(LoginDto loginDto)
    {
        var user = await context.Users.AsNoTracking()
            .Where(u => u.Email == loginDto.Email.ToLowerInvariant())
            .FirstOrDefaultAsync();

        if (user == null || !VerifyPassword(loginDto.Password, user.PasswordHash, user.PasswordSalt))
        {
            throw new UnauthorizedAccessException("Неверный email или пароль");
        }

        var token = jwtService.GenerateToken(user.Id, user.Email);
        var expiresAt = jwtService.GetTokenExpiration();

        return new AuthResponseDto
        {
            Token = token,
            TokenType = "Bearer",
            ExpiresAt = expiresAt,
            User = new UserDto
            {
                Id = user.Id,
                FirstName = user.FirstName,
                LastName = user.LastName,
                Email = user.Email
            }
        };
    }

    private static (string hash, string salt) HashPassword(string password)
    {
        var saltBytes = RandomNumberGenerator.GetBytes(32);
        var salt = Convert.ToBase64String(saltBytes);

        var hashBytes = Rfc2898DeriveBytes.Pbkdf2(
            password,
            saltBytes,
            100000,
            HashAlgorithmName.SHA256,
            32
        );

        var hash = Convert.ToBase64String(hashBytes);

        return (hash, salt);
    }

    private static bool VerifyPassword(string password, string storedHash, string storedSalt)
    {
        var saltBytes = Convert.FromBase64String(storedSalt);

        var hashBytes = Rfc2898DeriveBytes.Pbkdf2(
            password,
            saltBytes,
            100000,
            HashAlgorithmName.SHA256,
            32
        );

        var hash = Convert.ToBase64String(hashBytes);

        return hash == storedHash;
    }
}