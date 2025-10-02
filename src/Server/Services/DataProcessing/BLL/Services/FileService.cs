using System.Text;
using Core.Enums;
using Core.Models.Common;
using Core.Models.Files.Parameters;
using Core.Models.Files.Schema;
using Core.Models.Files.Sources;
using DataProcessing.BLL.Analayzers;
using DataProcessing.BLL.Interfaces;

namespace DataProcessing.BLL.Services;

internal class FileService : IFileService
{
    public FileType DetectFileType(Stream stream)
    {
        if (stream is not { CanRead: true })
            throw new ArgumentException("Stream is null or not readable.");

        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 1024, leaveOpen: true);
        var firstLine = reader.ReadLine();
        
        if (string.IsNullOrEmpty(firstLine))
            return FileType.Unknown;

        var content = firstLine.TrimStart();

        if (content.Length == 0)
            return FileType.Unknown;

        if (content.StartsWith("{") || content.StartsWith("["))
            return FileType.Json;

        if (content.StartsWith("<"))
            return FileType.Xml;

        var separators = new[] { ',', ';', '\t' };
        var commonSeparators = separators.Where(s => firstLine.Contains(s)).ToList();

        if (commonSeparators.Count != 1) 
            return FileType.Unknown;
        
        var sep = commonSeparators[0];
        var parts = firstLine.Split(sep);
        
        return parts.Length > 1 ? FileType.Csv : FileType.Unknown;
    }

    public JsonSourceDto GetJsonSchema(string fileUrl, Stream stream)
    {
        if (stream is not { CanRead: true })
            throw new ArgumentException("Stream is null or not readable.");

        using var countingStream = new CountingStream(stream);
        var schemaAnalyzer = new JsonSchemaAnalyzer();
        schemaAnalyzer.Analyze(countingStream);
        
        var fileSizeMb = countingStream.BytesRead / (1024 * 1024);

        var schemaDto = new JsonSchemaDto
        {
            SizeMb = fileSizeMb,
            RowCount = schemaAnalyzer.RowCount,
            FieldsCount = schemaAnalyzer.Fields.Count,
            Fields = schemaAnalyzer.Fields.Select(f =>
            {
                var isTopLevel = !f.Path.Contains('.');
                long nullCount;
                
                if (isTopLevel)
                {
                    nullCount = Math.Max(0, schemaAnalyzer.RowCount - f.TotalCount + f.NullCount);
                }
                else
                {
                    var firstDotIndex = f.Path.IndexOf('.');
                    var rootPath = f.Path.Substring(0, firstDotIndex);
                    
                    if (schemaAnalyzer.PathOccurrences.TryGetValue(rootPath, out var rootOccurrences))
                    {
                        var rootMissingCount = schemaAnalyzer.RowCount - rootOccurrences;
                        
                        nullCount = Math.Max(0, rootMissingCount + f.NullCount);
                    }
                    else
                    {
                        nullCount = f.NullCount;
                    }
                }
                
                var nullable = nullCount > 0;
                
                return new ColumnSchemaDto
                {
                    Name = f.Name,
                    Path = f.Path,
                    DataType = f.DataType,
                    Nullable = nullable,
                    Filter = null,
                    Sort = null,
                    SampleValues = f.SampleValues.Take(10).ToList(),
                    UniqueValues = f.UniqueValues.Count,
                    NullCount = nullCount,
                    Statistics = new StatisticsSchemaDto
                    {
                        Min = JsonSchemaAnalyzer.GetMin(f) ?? string.Empty,
                        Max = JsonSchemaAnalyzer.GetMax(f) ?? string.Empty,
                        Avg = JsonSchemaAnalyzer.GetAvg(f) ?? string.Empty
                    }
                };
            }).ToList()
        };

        return new JsonSourceDto
        {
            Uid = Guid.CreateVersion7(),
            Type = SourceType.File,
            Parameters = new FileParametersDto
            {
                Type = FileType.Json,
                Url = fileUrl
            },
            SchemaInfos = [schemaDto]
        };
    }

    public CsvSourceDto GetCsvSchema(string fileUrl, Stream stream)
    {
        if (stream is not { CanRead: true })
            throw new ArgumentException("Stream is null or not readable.");

        using var countingStream = new CountingStream(stream);
        
        var schemaAnalyzer = new CsvSchemaAnalyzer();
        schemaAnalyzer.AnalyzeWithAutoDelimiter(countingStream);
        
        var fileSizeMb = countingStream.BytesRead / (1024 * 1024);

        var schemaDto = new CsvSchemaDto
        {
            SizeMb = fileSizeMb,
            RowCount = schemaAnalyzer.RowCount,
            ColumnCount = schemaAnalyzer.Fields.Count,
            Columns = schemaAnalyzer.Fields.Select(f =>
            {
                var nullCount = schemaAnalyzer.RowCount - f.TotalCount + f.NullCount;
                var nullable = nullCount > 0;

                return new ColumnSchemaDto
                {
                    Name = f.Name,
                    Path = f.Name,
                    DataType = f.DataType,
                    Nullable = nullable,
                    Filter = null,
                    Sort = null,
                    SampleValues = f.SampleValues.Take(10).ToList(),
                    UniqueValues = f.UniqueValues.Count,
                    NullCount = nullCount,
                    Statistics = new StatisticsSchemaDto
                    {
                        Min = CsvSchemaAnalyzer.GetMin(f) ?? string.Empty,
                        Max = CsvSchemaAnalyzer.GetMax(f) ?? string.Empty,
                        Avg = CsvSchemaAnalyzer.GetAvg(f) ?? string.Empty
                    }
                };
            }).ToList()
        };

        return new CsvSourceDto
        {
            Uid = Guid.CreateVersion7(),
            Type = SourceType.File,
            Parameters = new CsvParametersDto
            {
                Type = FileType.Csv,
                Url = fileUrl,
                Delimiter = schemaAnalyzer.Delimiter.ToString()
            },
            SchemaInfos = [schemaDto]
        };
    }

    public XmlSourceDto GetXmlSchema(string fileUrl, Stream stream)
    {
        if (stream is not { CanRead: true })
            throw new ArgumentException("Stream is null or not readable.");

        using var countingStream = new CountingStream(stream);
        var schemaAnalyzer = new XmlSchemaAnalyzer();
        schemaAnalyzer.Analyze(countingStream);
        
        var fileSizeMb = countingStream.BytesRead / (1024 * 1024);

        var schemaDto = new XmlSchemaDto
        {
            SizeMb = fileSizeMb,
            RowCount = schemaAnalyzer.RowCount,
            ElementCount = schemaAnalyzer.Fields.Count,
            Elements = schemaAnalyzer.Fields.Select(f =>
            {
                var isTopLevel = !f.Path.Contains('.');
                long nullCount;
                
                if (isTopLevel)
                {
                    nullCount = Math.Max(0, schemaAnalyzer.RowCount - f.TotalCount + f.NullCount);
                }
                else
                {
                    var firstDotIndex = f.Path.IndexOf('.');
                    var rootPath = f.Path.Substring(0, firstDotIndex);
                    
                    if (schemaAnalyzer.PathOccurrences.TryGetValue(rootPath, out var rootOccurrences))
                    {
                        var rootMissingCount = schemaAnalyzer.RowCount - rootOccurrences;
                        nullCount = Math.Max(0, rootMissingCount + f.NullCount);
                    }
                    else
                    {
                        nullCount = f.NullCount;
                    }
                }
                
                var nullable = nullCount > 0;

                return new ColumnSchemaDto
                {
                    Name = f.Name,
                    Path = f.Path,
                    DataType = f.DataType,
                    Nullable = nullable,
                    Filter = null,
                    Sort = null,
                    SampleValues = f.SampleValues.Take(10).ToList(),
                    UniqueValues = f.UniqueValues.Count,
                    NullCount = nullCount,
                    Statistics = new StatisticsSchemaDto
                    {
                        Min = XmlSchemaAnalyzer.GetMin(f) ?? string.Empty,
                        Max = XmlSchemaAnalyzer.GetMax(f) ?? string.Empty,
                        Avg = XmlSchemaAnalyzer.GetAvg(f) ?? string.Empty
                    }
                };
            }).ToList()
        };

        return new XmlSourceDto
        {
            Uid = Guid.CreateVersion7(),
            Type = SourceType.File,
            Parameters = new FileParametersDto
            {
                Type = FileType.Xml,
                Url = fileUrl
            },
            SchemaInfos = [schemaDto]
        };
    }
}
