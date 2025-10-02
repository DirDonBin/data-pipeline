namespace Core.Models.Database.Schema;

public record TableSchemaDto
{
    public required string Name { get; init; }
    public required List<DatabaseColumnSchemaDto> Columns { get; init; }
    public required List<ForeignKeyDto> ForeignKeys { get; init; }
    public required List<IndexDto> Indexes { get; init; }
}
