using ClickHouse.Client.ADO;
using ClickHouse.Client.ADO.Parameters;
using Core.Enums;
using Core.Models.Common;
using Core.Models.Database.Parameters;
using Core.Models.Database.Schema;
using Core.Models.Database.Sources;
using DataProcessing.BLL.Interfaces;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace DataProcessing.BLL.Services;

internal class DatabaseService : IDatabaseService
{
    private readonly ILogger<DatabaseService> _logger;

    public DatabaseService(ILogger<DatabaseService> logger)
    {
        _logger = logger;
    }

    public async Task<DatabaseSourceDto> GetPostgreSqlSchemaAsync(DatabaseParametersDto parameters)
    {
        var connectionString = BuildConnectionString(parameters);
        
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        var tableNames = await GetTableNamesAsync(connection, parameters.Schema);
        var tables = new List<TableSchemaDto>();
        long totalSize = 0;
        long totalRows = 0;

        foreach (var tableName in tableNames)
        {
            var columns = await GetColumnsAsync(connection, parameters.Schema, tableName);
            var foreignKeys = await GetForeignKeysAsync(connection, parameters.Schema, tableName);
            var indexes = await GetIndexesAsync(connection, parameters.Schema, tableName);
            var tableStats = await GetTableStatsAsync(connection, parameters.Schema, tableName);
            
            tables.Add(new TableSchemaDto
            {
                Name = tableName,
                Columns = columns,
                ForeignKeys = foreignKeys,
                Indexes = indexes
            });
            
            totalSize += tableStats.SizeMb;
            totalRows += tableStats.RowCount;
        }

        var schemaDto = new DatabaseSchemaDto
        {
            SizeMb = totalSize,
            RowCount = totalRows,
            TableCount = tables.Count,
            Tables = tables
        };

        return new DatabaseSourceDto
        {
            Uid = Guid.CreateVersion7(),
            Type = SourceType.Database,
            Parameters = parameters,
            SchemaInfos = [schemaDto]
        };
    }

    private static string BuildConnectionString(DatabaseParametersDto parameters)
    {
        return $"Host={parameters.Host};Port={parameters.Port};Database={parameters.Database};Username={parameters.User};Password={parameters.Password}";
    }

    private static async Task<List<string>> GetTableNamesAsync(NpgsqlConnection connection, string schema)
    {
        var tableNames = new List<string>();
        
        var query = @"
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = @schema 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name";

        await using var cmd = new NpgsqlCommand(query, connection);
        cmd.Parameters.AddWithValue("schema", schema);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tableNames.Add(reader.GetString(0));
        }

        return tableNames;
    }

    private async Task<List<DatabaseColumnSchemaDto>> GetColumnsAsync(NpgsqlConnection connection, string schema, string tableName)
    {
        var columns = new List<DatabaseColumnSchemaDto>();
        var columnInfos = new List<(string Name, string DataType, bool IsNullable, bool IsPrimary)>();

        var query = @"
            SELECT 
                c.column_name,
                c.data_type,
                c.is_nullable,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                    ON tc.constraint_name = ku.constraint_name
                    AND tc.table_schema = ku.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = @schema
                    AND tc.table_name = @tableName
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_schema = @schema 
            AND c.table_name = @tableName
            ORDER BY c.ordinal_position";

        await using var cmd = new NpgsqlCommand(query, connection);
        cmd.Parameters.AddWithValue("schema", schema);
        cmd.Parameters.AddWithValue("tableName", tableName);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columnInfos.Add((
                reader.GetString(0),
                reader.GetString(1),
                reader.GetString(2) == "YES",
                reader.GetBoolean(3)
            ));
        }
        await reader.CloseAsync();

        foreach (var (name, dataType, isNullable, isPrimary) in columnInfos)
        {
            var stats = await GetColumnStatsAsync(connection, schema, tableName, name);

            columns.Add(new DatabaseColumnSchemaDto
            {
                Name = name,
                Path = name,
                DataType = dataType,
                Nullable = isNullable,
                IsPrimary = isPrimary,
                Filter = null,
                Sort = null,
                SampleValues = stats.SampleValues,
                UniqueValues = stats.UniqueValues,
                NullCount = stats.NullCount,
                Statistics = stats.Statistics
            });
        }

        return columns;
    }

    private async Task<(List<string> SampleValues, long UniqueValues, long NullCount, StatisticsSchemaDto Statistics)> 
        GetColumnStatsAsync(NpgsqlConnection connection, string schema, string tableName, string columnName)
    {
        var sampleValues = new List<string>();
        long uniqueValues = 0;
        long nullCount = 0;
        var statistics = new StatisticsSchemaDto { Min = string.Empty, Max = string.Empty, Avg = string.Empty };

        try
        {
            var quotedSchema = QuoteIdentifier(schema);
            var quotedTable = QuoteIdentifier(tableName);
            var quotedColumn = QuoteIdentifier(columnName);
            
            var statsQuery = $@"
                SELECT 
                    COUNT(DISTINCT {quotedColumn}) as unique_count,
                    COUNT(*) FILTER (WHERE {quotedColumn} IS NULL) as null_count,
                    MIN({quotedColumn}::text) as min_val,
                    MAX({quotedColumn}::text) as max_val
                FROM {quotedSchema}.{quotedTable}";

            await using var statsCmd = new NpgsqlCommand(statsQuery, connection);
            await using var statsReader = await statsCmd.ExecuteReaderAsync();
            
            if (await statsReader.ReadAsync())
            {
                uniqueValues = statsReader.IsDBNull(0) ? 0 : statsReader.GetInt64(0);
                nullCount = statsReader.IsDBNull(1) ? 0 : statsReader.GetInt64(1);
                statistics = new StatisticsSchemaDto
                {
                    Min = statsReader.IsDBNull(2) ? string.Empty : statsReader.GetString(2),
                    Max = statsReader.IsDBNull(3) ? string.Empty : statsReader.GetString(3),
                    Avg = string.Empty
                };
            }

            await statsReader.CloseAsync();

            var sampleQuery = $@"
                SELECT DISTINCT {quotedColumn}::text 
                FROM {quotedSchema}.{quotedTable} 
                WHERE {quotedColumn} IS NOT NULL 
                LIMIT 5";

            await using var sampleCmd = new NpgsqlCommand(sampleQuery, connection);
            await using var sampleReader = await sampleCmd.ExecuteReaderAsync();
            
            while (await sampleReader.ReadAsync())
            {
                sampleValues.Add(sampleReader.GetString(0));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get statistics for column {Column} in table {Table}", columnName, tableName);
        }

        return (sampleValues, uniqueValues, nullCount, statistics);
    }

    private static async Task<List<ForeignKeyDto>> GetForeignKeysAsync(NpgsqlConnection connection, string schema, string tableName)
    {
        var foreignKeys = new List<ForeignKeyDto>();

        var query = @"
            SELECT
                tc.constraint_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = @schema
                AND tc.table_name = @tableName";

        await using var cmd = new NpgsqlCommand(query, connection);
        cmd.Parameters.AddWithValue("schema", schema);
        cmd.Parameters.AddWithValue("tableName", tableName);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            foreignKeys.Add(new ForeignKeyDto
            {
                Name = reader.GetString(0),
                ForeignTable = tableName,
                ForeignColumn = reader.GetString(1),
                PrimaryTable = reader.GetString(2),
                PrimaryColumn = reader.GetString(3)
            });
        }

        return foreignKeys;
    }

    private static async Task<List<IndexDto>> GetIndexesAsync(NpgsqlConnection connection, string schema, string tableName)
    {
        var indexes = new List<IndexDto>();

        var query = @"
            SELECT
                i.relname as index_name,
                ix.indisunique as is_unique,
                ix.indisclustered as is_clustered,
                array_agg(a.attname ORDER BY array_position(ix.indkey, a.attnum)) as column_names
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relkind = 'r'
                AND n.nspname = @schema
                AND t.relname = @tableName
                AND NOT ix.indisprimary
            GROUP BY i.relname, ix.indisunique, ix.indisclustered";

        await using var cmd = new NpgsqlCommand(query, connection);
        cmd.Parameters.AddWithValue("schema", schema);
        cmd.Parameters.AddWithValue("tableName", tableName);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            indexes.Add(new IndexDto
            {
                Name = reader.GetString(0),
                IsUnique = reader.GetBoolean(1),
                IsClustered = reader.GetBoolean(2),
                Columns = ((string[])reader.GetValue(3)).ToList()
            });
        }

        return indexes;
    }

    private async Task<(long SizeMb, long RowCount)> GetTableStatsAsync(NpgsqlConnection connection, string schema, string tableName)
    {
        var quotedSchema = QuoteIdentifier(schema);
        var quotedTable = QuoteIdentifier(tableName);
        
        var query = $@"
            SELECT 
                pg_total_relation_size({quotedSchema}.{quotedTable}) / (1024 * 1024) as size_mb,
                (SELECT COUNT(*) FROM {quotedSchema}.{quotedTable}) as row_count";

        await using var cmd = new NpgsqlCommand(query, connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        if (await reader.ReadAsync())
        {
            var sizeMb = reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
            var rowCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1);
            return (sizeMb, rowCount);
        }

        return (0, 0);
    }

    private static string QuoteIdentifier(string identifier)
    {
        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    public async Task<DatabaseSourceDto> GetClickHouseSchemaAsync(DatabaseParametersDto parameters)
    {
        var connectionString = BuildClickHouseConnectionString(parameters);
        
        await using var connection = new ClickHouseConnection(connectionString);
        await connection.OpenAsync();

        var tableNames = await GetClickHouseTableNamesAsync(connection, parameters.Database);
        var tables = new List<TableSchemaDto>();
        long totalSize = 0;
        long totalRows = 0;

        foreach (var tableName in tableNames)
        {
            var columns = await GetClickHouseColumnsAsync(connection, parameters.Database, tableName);
            var tableStats = await GetClickHouseTableStatsAsync(connection, parameters.Database, tableName);
            
            tables.Add(new TableSchemaDto
            {
                Name = tableName,
                Columns = columns,
                ForeignKeys = [],
                Indexes = []
            });
            
            totalSize += tableStats.SizeMb;
            totalRows += tableStats.RowCount;
        }

        var schemaDto = new DatabaseSchemaDto
        {
            SizeMb = totalSize,
            RowCount = totalRows,
            TableCount = tables.Count,
            Tables = tables
        };

        return new DatabaseSourceDto
        {
            Uid = Guid.CreateVersion7(),
            Type = SourceType.Database,
            Parameters = parameters,
            SchemaInfos = [schemaDto]
        };
    }

    private static string BuildClickHouseConnectionString(DatabaseParametersDto parameters)
    {
        return $"Host={parameters.Host};Port={parameters.Port};Database={parameters.Database};Username={parameters.User};Password={parameters.Password}";
    }

    private static async Task<List<string>> GetClickHouseTableNamesAsync(ClickHouseConnection connection, string database)
    {
        var tableNames = new List<string>();
        
        var query = "SELECT name FROM system.tables WHERE database = {database:String} AND engine NOT LIKE '%View%' ORDER BY name";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "database", Value = database });

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tableNames.Add(reader.GetString(0));
        }

        return tableNames;
    }

    private async Task<List<DatabaseColumnSchemaDto>> GetClickHouseColumnsAsync(ClickHouseConnection connection, string database, string tableName)
    {
        var columns = new List<DatabaseColumnSchemaDto>();
        var columnInfos = new List<(string Name, string DataType, bool IsNullable, bool IsPrimary)>();

        var query = @"
            SELECT 
                name,
                type,
                CASE WHEN type LIKE '%Nullable%' THEN 1 ELSE 0 END as is_nullable,
                is_in_primary_key
            FROM system.columns
            WHERE database = {database:String} 
            AND table = {table:String}
            ORDER BY position";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "database", Value = database });
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "table", Value = tableName });

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            columnInfos.Add((
                reader.GetString(0),
                reader.GetString(1),
                reader.GetByte(2) == 1,
                reader.GetByte(3) == 1
            ));
        }
        await reader.CloseAsync();

        foreach (var (name, dataType, isNullable, isPrimary) in columnInfos)
        {
            var stats = await GetClickHouseColumnStatsAsync(connection, database, tableName, name);

            columns.Add(new DatabaseColumnSchemaDto
            {
                Name = name,
                Path = name,
                DataType = dataType,
                Nullable = isNullable,
                IsPrimary = isPrimary,
                Filter = null,
                Sort = null,
                SampleValues = stats.SampleValues,
                UniqueValues = stats.UniqueValues,
                NullCount = stats.NullCount,
                Statistics = stats.Statistics
            });
        }

        return columns;
    }

    private async Task<(List<string> SampleValues, long UniqueValues, long NullCount, StatisticsSchemaDto Statistics)> 
        GetClickHouseColumnStatsAsync(ClickHouseConnection connection, string database, string tableName, string columnName)
    {
        var sampleValues = new List<string>();
        long uniqueValues = 0;
        long nullCount = 0;
        var statistics = new StatisticsSchemaDto { Min = string.Empty, Max = string.Empty, Avg = string.Empty };

        try
        {
            var quotedColumn = QuoteClickHouseIdentifier(columnName);
            
            var statsQuery = $@"
                SELECT 
                    uniq({quotedColumn}) as unique_count,
                    countIf(isNull({quotedColumn})) as null_count,
                    toString(min({quotedColumn})) as min_val,
                    toString(max({quotedColumn})) as max_val
                FROM {{database:String}}.{{table:String}}";

            await using var statsCmd = connection.CreateCommand();
            statsCmd.CommandText = statsQuery;
            statsCmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "database", Value = database });
            statsCmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "table", Value = tableName });
            
            await using var statsReader = await statsCmd.ExecuteReaderAsync();
            
            if (await statsReader.ReadAsync())
            {
                uniqueValues = statsReader.IsDBNull(0) ? 0 : statsReader.GetInt64(0);
                nullCount = statsReader.IsDBNull(1) ? 0 : statsReader.GetInt64(1);
                statistics = new StatisticsSchemaDto
                {
                    Min = statsReader.IsDBNull(2) ? string.Empty : statsReader.GetString(2),
                    Max = statsReader.IsDBNull(3) ? string.Empty : statsReader.GetString(3),
                    Avg = string.Empty
                };
            }

            await statsReader.CloseAsync();

            var sampleQuery = $@"
                SELECT DISTINCT toString({quotedColumn}) 
                FROM {{database:String}}.{{table:String}} 
                WHERE NOT isNull({quotedColumn}) 
                LIMIT 5";

            await using var sampleCmd = connection.CreateCommand();
            sampleCmd.CommandText = sampleQuery;
            sampleCmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "database", Value = database });
            sampleCmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "table", Value = tableName });
            
            await using var sampleReader = await sampleCmd.ExecuteReaderAsync();
            
            while (await sampleReader.ReadAsync())
            {
                sampleValues.Add(sampleReader.GetString(0));
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get statistics for column {Column} in table {Table}", columnName, tableName);
        }

        return (sampleValues, uniqueValues, nullCount, statistics);
    }

    private static async Task<(long SizeMb, long RowCount)> GetClickHouseTableStatsAsync(ClickHouseConnection connection, string database, string tableName)
    {
        var query = @"
            SELECT 
                sum(bytes) / (1024 * 1024) as size_mb,
                sum(rows) as row_count
            FROM system.parts
            WHERE database = {database:String}
            AND table = {table:String}
            AND active = 1";

        await using var cmd = connection.CreateCommand();
        cmd.CommandText = query;
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "database", Value = database });
        cmd.Parameters.Add(new ClickHouseDbParameter { ParameterName = "table", Value = tableName });
        
        await using var reader = await cmd.ExecuteReaderAsync();

        if (await reader.ReadAsync())
        {
            var sizeMb = reader.IsDBNull(0) ? 0 : reader.GetInt64(0);
            var rowCount = reader.IsDBNull(1) ? 0 : reader.GetInt64(1);
            return (sizeMb, rowCount);
        }

        return (0, 0);
    }

    private static string QuoteClickHouseIdentifier(string identifier)
    {
        return $"`{identifier.Replace("`", "``")}`";
    }
}
