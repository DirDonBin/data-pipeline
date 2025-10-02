using System.Security.Claims;
using Core.Models.Pipelines;
using Internal.BLL.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace Internal.API.Endpoints;

public static class PipelineEndpoints
{
    public static void MapPipelineEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/pipelines")
            .WithTags("Pipelines")
            .RequireAuthorization();

        group.MapPost("/", async (
            [FromBody] CreatePipelineDto createPipelineDto,
            [FromServices] IPipelineService pipelineService,
            HttpContext httpContext,
            CancellationToken cancellationToken) =>
        {
            try
            {
                var userIdStr = httpContext.User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
                
                if (string.IsNullOrEmpty(userIdStr) || !Guid.TryParse(userIdStr, out var userId))
                {
                    return Results.Unauthorized();
                }

                var result = await pipelineService.CreatePipelineAsync(createPipelineDto, userId, cancellationToken);
                return Results.Ok(result);
            }
            catch (Exception ex)
            {
                return Results.Problem(
                    detail: ex.Message,
                    statusCode: 500,
                    title: "Ошибка создания пайплайна"
                );
            }
        })
        .WithName("CreatePipeline")
        .WithSummary("Создание нового пайплайна")
        .WithDescription("Создает новый пайплайн")
        .Produces<CreatePipelineResponseDto>()
        .Produces(401)
        .Produces(500);
    }
}
