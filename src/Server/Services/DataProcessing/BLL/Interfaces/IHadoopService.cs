using Core.Enums;

namespace DataProcessing.BLL.Interfaces;

public interface IHadoopService
{
    Task<string> UploadFileAsync(Stream file, string fileName, FileType fileType);
    Task<Stream> ReadFileAsync(string fileUrl);
    Task<bool> FileExistsAsync(string fileUrl);
}
