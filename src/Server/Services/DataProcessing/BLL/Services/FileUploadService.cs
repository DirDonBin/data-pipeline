using Core.Enums;
using Core.Models;
using Core.Models.Pipelines;
using DataProcessing.BLL.Interfaces;
using Microsoft.Extensions.Logging;

namespace DataProcessing.BLL.Services;

internal class FileUploadService(
    IHadoopService hadoopService,
    IFileService fileService,
    ILogger<FileUploadService> logger)
    : IFileUploadService
{
    public async Task<FileNodeConfigDto> UploadFileAsync(Stream fileStream, string fileName)
    {
        if (fileStream is not { CanRead: true })
            throw new ArgumentException("Stream is null or not readable.", nameof(fileStream));

        if (string.IsNullOrWhiteSpace(fileName))
            throw new ArgumentException("File name cannot be empty.", nameof(fileName));

        logger.LogInformation("Starting file upload: {FileName}", fileName);

        FileType fileType;

        fileType = fileService.DetectFileType(fileStream);
        fileStream.Position = 0;

        if (fileType == FileType.Unknown)
        {
            logger.LogWarning("Could not detect file type for {FileName}, defaulting to CSV", fileName);
            fileType = FileType.Csv;
        }

        logger.LogInformation("Detected file type: {FileType} for {FileName}", fileType, fileName);

        var fileUrl = await hadoopService.UploadFileAsync(fileStream, fileName, fileType);

        logger.LogInformation("Successfully uploaded file {FileName} to {FileUrl}", fileName, fileUrl);

        return new FileNodeConfigDto
        {
            FileUrl = fileUrl,
            FileType = fileType
        };
    }
}
