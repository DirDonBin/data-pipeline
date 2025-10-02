namespace Core.Models.Database.Schema;

public record ForeignKeyDto
{
    public required string Name { get; init; }
    public required string PrimaryTable { get; init; }
    public required string PrimaryColumn { get; init; }
    public required string ForeignTable { get; init; }
    public required string ForeignColumn { get; init; }
}
