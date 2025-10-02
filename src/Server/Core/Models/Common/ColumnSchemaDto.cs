using Core.Enums;

namespace Core.Models.Common;

public record ColumnSchemaDto
{
    public required string Name { get; init; }
    public required string Path { get; init; }
    public required string DataType { get; init; }
    public required bool Nullable { get; init; }
    public required FilterSchemaDto? Filter { get; init; }
    public required SortDirection? Sort { get; init; }
    public required List<string> SampleValues { get; init; }
    public required long UniqueValues { get; init; }
    public required long NullCount { get; init; }
    public required StatisticsSchemaDto Statistics { get; init; }
}
