using Core.Models.Common;

namespace Core.Models.Files.Schema;

public record CsvSchemaDto : SchemaDto
{
    public required long ColumnCount { get; init; }
    public required List<ColumnSchemaDto> Columns { get; init; }
}
