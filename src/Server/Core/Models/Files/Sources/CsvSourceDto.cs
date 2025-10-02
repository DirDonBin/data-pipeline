using Core.Models.Files.Parameters;
using Core.Models.Files.Schema;

namespace Core.Models.Files.Sources;

public record CsvSourceDto : SourceDto
{
    public required CsvParametersDto Parameters { get; init; }
    public required List<CsvSchemaDto> SchemaInfos { get; init; }
}
