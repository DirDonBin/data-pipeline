using Core.Enums;

namespace Core.Models.Files.Parameters;

public record FileParametersDto : ParametersDto
{
    public required FileType Type { get; init; }
    public required string Url { get; init; }
}
