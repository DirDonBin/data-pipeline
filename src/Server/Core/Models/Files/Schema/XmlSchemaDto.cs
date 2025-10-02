using Core.Models.Common;

namespace Core.Models.Files.Schema;

public record XmlSchemaDto : SchemaDto
{
    public required long ElementCount { get; init; }
    public required List<ColumnSchemaDto> Elements { get; init; }
}
