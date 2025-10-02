using Core.Models.Common;

namespace Core.Models.Database.Schema;

public record DatabaseColumnSchemaDto : ColumnSchemaDto
{
    public required bool IsPrimary { get; init; }
}
