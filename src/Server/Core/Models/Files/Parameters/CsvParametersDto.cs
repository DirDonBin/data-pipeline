namespace Core.Models.Files.Parameters;

public record CsvParametersDto : FileParametersDto
{
    public required string Delimiter { get; init; }
}
