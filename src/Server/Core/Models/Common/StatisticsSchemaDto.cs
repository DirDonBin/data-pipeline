namespace Core.Models.Common;

public record StatisticsSchemaDto
{
    public required string Min { get; init; }
    public required string Max { get; init; }
    public required string Avg { get; init; }
}
