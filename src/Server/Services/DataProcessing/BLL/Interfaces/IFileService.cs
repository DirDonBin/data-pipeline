using Core.Enums;
using Core.Models;
using Core.Models.Files.Sources;

namespace DataProcessing.BLL.Interfaces;

public interface IFileService
{
    FileType DetectFileType(Stream stream);
    JsonSourceDto GetJsonSchema(string fileUrl, Stream stream);
    CsvSourceDto GetCsvSchema(string fileUrl, Stream stream);
    XmlSourceDto GetXmlSchema(string fileUrl, Stream stream);
}