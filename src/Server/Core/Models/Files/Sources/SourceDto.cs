using System.Text.Json.Serialization;
using Core.Enums;
using Core.Models.Database.Sources;

namespace Core.Models.Files.Sources;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(CsvSourceDto), typeDiscriminator: "csv")]
[JsonDerivedType(typeof(JsonSourceDto), typeDiscriminator: "json")]
[JsonDerivedType(typeof(XmlSourceDto), typeDiscriminator: "xml")]
[JsonDerivedType(typeof(DatabaseSourceDto), typeDiscriminator: "database")]
public record SourceDto
{
    public required Guid Uid { get; init; }
    public required SourceType Type { get; init; }
}
