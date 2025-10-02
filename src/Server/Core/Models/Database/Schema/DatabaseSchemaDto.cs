using Core.Models.Files.Schema;

namespace Core.Models.Database.Schema;

public record DatabaseSchemaDto : SchemaDto
{
    public required long TableCount { get; init; }
    public required List<TableSchemaDto> Tables { get; init; }
}
