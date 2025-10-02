using Core.Enums;

namespace Core.Models.Common;

public record FilterSchemaDto
{
    public required OperationType Operation { get; init; }
    public required string Value { get; init; }
}
