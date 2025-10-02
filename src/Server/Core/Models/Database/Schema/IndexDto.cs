namespace Core.Models.Database.Schema;

public record IndexDto
{
    public required string Name { get; init; }
    public required bool IsUnique { get; init; }
    public required bool IsClustered { get; init; }
    public required List<string> Columns { get; init; }
}
