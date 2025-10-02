using Core.Models.Common;

namespace Core.Models.Files.Schema;

public record JsonSchemaDto : SchemaDto
{
    public required long FieldsCount { get; init; }
    public required List<ColumnSchemaDto> Fields { get; init; }
}
