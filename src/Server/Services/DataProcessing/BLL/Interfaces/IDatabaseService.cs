using Core.Models;
using Core.Models.Database.Parameters;
using Core.Models.Database.Sources;

namespace DataProcessing.BLL.Interfaces;

public interface IDatabaseService
{
    Task<DatabaseSourceDto> GetPostgreSqlSchemaAsync(DatabaseParametersDto parameters);
    Task<DatabaseSourceDto> GetClickHouseSchemaAsync(DatabaseParametersDto parameters);
}
