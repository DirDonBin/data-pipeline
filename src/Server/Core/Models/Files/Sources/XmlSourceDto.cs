using Core.Models.Files.Parameters;
using Core.Models.Files.Schema;

namespace Core.Models.Files.Sources;

public record XmlSourceDto : SourceDto
{
    public required FileParametersDto Parameters { get; init; }
    public required List<XmlSchemaDto> SchemaInfos { get; init; }
}
