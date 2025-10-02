using Core.Models.Database.Parameters;
using Core.Models.Database.Schema;
using Core.Models.Files.Sources;

namespace Core.Models.Database.Sources;

public record DatabaseSourceDto : SourceDto
{
    public required DatabaseParametersDto Parameters { get; init; }
    public required List<DatabaseSchemaDto> SchemaInfos { get; init; }
}
