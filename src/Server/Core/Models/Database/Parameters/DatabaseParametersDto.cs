using Core.Enums;
using Core.Models.Files.Parameters;

namespace Core.Models.Database.Parameters;

public record DatabaseParametersDto : ParametersDto
{
    public required DatabaseType Type { get; init; }
    public required string Host { get; init; }
    public required int Port { get; init; }
    public required string Database { get; init; }
    public required string User { get; init; }
    public required string Password { get; init; }
    public required string Schema { get; init; }
}
