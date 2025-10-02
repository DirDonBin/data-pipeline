namespace Core.Models.Files.Schema;

public record SchemaDto
{
    public required long SizeMb { get; init; }
    public required long RowCount { get; init; }
}
