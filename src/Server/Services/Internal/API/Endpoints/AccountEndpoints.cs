using Core.Models;
using Core.Models.Auth;
using Internal.BLL.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace Internal.API.Endpoints;

public static class AccountEndpoints
{
    // ReSharper disable once CognitiveComplexity
    public static void MapAccountEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/accounts")
            .WithTags("Accounts");

        group.MapPost("/sign-up", async (
            [FromBody] RegisterDto registerDto,
            [FromServices] IAccountService accountService) =>
        {
            try
            {
                var result = await accountService.RegisterAsync(registerDto);
                return Results.Ok(result);
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
            catch (Exception ex)
            {
                return Results.Problem(
                    detail: ex.Message,
                    statusCode: 500,
                    title: "Ошибка регистрации"
                );
            }
        })
        .WithName("SignUp")
        .WithSummary("Регистрация нового пользователя")
        .Produces<AuthResponseDto>()
        .Produces(400)
        .Produces(500);

        group.MapPost("/sign-in", async (
            [FromBody] LoginDto loginDto,
            [FromServices] IAccountService accountService) =>
        {
            try
            {
                var result = await accountService.LoginAsync(loginDto);
                return Results.Ok(result);
            }
            catch (UnauthorizedAccessException)
            {
                return Results.Unauthorized();
            }
            catch (Exception ex)
            {
                return Results.Problem(
                    detail: ex.Message,
                    statusCode: 500,
                    title: "Ошибка входа"
                );
            }
        })
        .WithName("SignIn")
        .WithSummary("Вход пользователя")
        .Produces<AuthResponseDto>()
        .Produces(401)
        .Produces(500);

        group.MapGet("/me", (HttpContext httpContext) =>
        {
            var userIdStr = httpContext.User.FindFirst(System.Security.Claims.ClaimTypes.NameIdentifier)?.Value;
            var email = httpContext.User.FindFirst(System.Security.Claims.ClaimTypes.Email)?.Value;
            var firstName = httpContext.User.FindFirst(System.Security.Claims.ClaimTypes.GivenName)?.Value;
            var lastName = httpContext.User.FindFirst(System.Security.Claims.ClaimTypes.Surname)?.Value;
            
            if (string.IsNullOrEmpty(userIdStr) || !Guid.TryParse(userIdStr, out var userId))
            {
                return Results.Unauthorized();
            }
            
            var userDto = new UserDto
            {
                Id = userId,
                FirstName = firstName ?? string.Empty,
                LastName = lastName ?? string.Empty,
                Email = email ?? string.Empty
            };
            
            return Results.Ok(userDto);
        })
        .RequireAuthorization()
        .WithName("GetCurrentUser")
        .WithSummary("Получить информацию о текущем пользователе")
        .Produces<UserDto>()
        .Produces(401);
    }
}