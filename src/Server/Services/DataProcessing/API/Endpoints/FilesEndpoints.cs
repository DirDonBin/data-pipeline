using Core.Models;
using Core.Models.Pipelines;
using DataProcessing.BLL.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace DataProcessing.API.Endpoints;

public static class FilesEndpoints
{
    public static IEndpointRouteBuilder MapFileEndpoints(this IEndpointRouteBuilder endpoints)
    {
        var group = endpoints.MapGroup("/files").WithTags("Process");

        group.MapPost("test", async ([FromForm] IFormFileCollection files, IProcessService processService) =>
        {
            if (files.Count == 0)
                return Results.BadRequest("No files provided");

            if (files.Any(x => x.Length == 0))
                return Results.BadRequest("File is empty");

            await processService.GenerateScheme(files.ToList());

            return Results.NoContent();
        })
        .WithSummary("Test")
        .WithName("Test")
        .WithMetadata(new RequestFormLimitsAttribute { MultipartBodyLengthLimit = 107_374_182_400 })
        .DisableAntiforgery()
        .Accepts<IFormFileCollection>("multipart/form-data");

        group.MapPost("upload", async (IFormFile file, IFileUploadService fileUploadService, HttpContext httpContext) =>
        {
            if (file.Length == 0)
                return Results.BadRequest("File is required and cannot be empty");

            if (string.IsNullOrWhiteSpace(file.FileName))
                return Results.BadRequest("File name is required");

            using var stream = file.OpenReadStream();

            var fileNodeConfig = await fileUploadService.UploadFileAsync(stream, file.FileName);

            return Results.Created($"/files/upload", fileNodeConfig);
        })
        .WithSummary("Загрузка файла в Hadoop")
        .WithName("UploadFile")
        .WithMetadata(new RequestFormLimitsAttribute { MultipartBodyLengthLimit = 107_374_182_400 })
        .DisableAntiforgery()
        .Accepts<IFormFile>("multipart/form-data")
        .Produces<FileNodeConfigDto>(StatusCodes.Status201Created)
        .RequireAuthorization();

        return endpoints;
    }
}