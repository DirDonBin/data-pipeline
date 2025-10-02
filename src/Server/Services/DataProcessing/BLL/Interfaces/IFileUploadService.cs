using Core.Models;
using Core.Models.Pipelines;

namespace DataProcessing.BLL.Interfaces;

public interface IFileUploadService
{
    Task<FileNodeConfigDto> UploadFileAsync(Stream fileStream, string fileName);
}
