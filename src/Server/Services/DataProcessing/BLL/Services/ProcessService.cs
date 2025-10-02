using Core.Enums;
using Core.Models;
using Core.Models.Database.Parameters;
using Core.Models.Database.Sources;
using Core.Models.Files.Sources;
using Core.Models.Kafka;
using Core.Models.Pipelines;
using DataProcessing.BLL.Interfaces;
using DataProcessing.BLL.Producers;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace DataProcessing.BLL.Services;

internal class ProcessService(
    IFileService fileService, 
    AiRequestProducer aiRequestProducer, 
    IHadoopService hadoopService,
    IDatabaseService databaseService,
    ILogger<ProcessService> logger) : IProcessService
{
    public async Task GenerateScheme(List<IFormFile> files, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Начало обработки {Count} файлов", files.Count);
        
        var request = new GenerateAiRequest()
        {
            RequestUid = Guid.NewGuid(),
            PipelineUid = Guid.CreateVersion7(),
            Sources = [],
            Targets = []
        };
        
        foreach (var formFile in files)
        {
            logger.LogInformation("Обработка файла: {FileName}", formFile.FileName);
            
            FileType fileType;
            string fileUrl;
            
            {
                await using var file = formFile.OpenReadStream();
                fileType = fileService.DetectFileType(file);
            }
            
            {
                await using var file = formFile.OpenReadStream();
                fileUrl = await hadoopService.UploadFileAsync(file, formFile.FileName, fileType);
            }
            
            await using var hdfsStream = await hadoopService.ReadFileAsync(fileUrl);

            SourceDto? source = fileType switch
            {
                FileType.Csv => fileService.GetCsvSchema(fileUrl, hdfsStream),
                FileType.Json => fileService.GetJsonSchema(fileUrl, hdfsStream),
                FileType.Xml => fileService.GetXmlSchema(fileUrl, hdfsStream),
                _ => throw new NotSupportedException($"File type {fileType} is not supported")
            };

            request.Sources.Add(source);
            logger.LogInformation("Файл {FileName} успешно обработан", formFile.FileName);
        }

        SourceDto target = new DatabaseSourceDto()
        {
            Uid = Guid.CreateVersion7(),
            Type = SourceType.Database,
            Parameters = new()
            {
                Type = DatabaseType.PostgreSql,
                Host = "localhost",
                Port = 5432,
                Database = "test",
                User = "postgres",
                Password = "password",
                Schema = "public"
            },
            SchemaInfos = []
        };
        
        request.Targets.Add(target);
        
        await aiRequestProducer.SendAsync(request.RequestUid.ToString(), request, cancellationToken);
        logger.LogInformation("Обработка файлов завершена. RequestUid: {RequestUid}", request.RequestUid);
    }

    public async Task<GenerateAiRequest> GenerateScheme(PipelineStartMessage pipelineMessage, CancellationToken cancellationToken = default)
    {
        logger.LogInformation("Начало генерации схемы для пайплайна {PipelineId}. Источников: {SourceCount}, Таргетов: {TargetCount}", 
            pipelineMessage.PipelineId, pipelineMessage.Sources.Count, pipelineMessage.Targets.Count);
        
        var request = new GenerateAiRequest
        {
            RequestUid = pipelineMessage.RequestUid,
            PipelineUid = pipelineMessage.PipelineId,
            Sources = [],
            Targets = []
        };
        
        foreach (var sourceNode in pipelineMessage.Sources)
        {
            logger.LogInformation("Обработка источника: {NodeName} ({NodeType})", 
                sourceNode.Name, sourceNode.DataNodeType);
                
            var source = await ProcessDataNode(sourceNode, cancellationToken);
            if (source != null)
            {
                request.Sources.Add(source);
                logger.LogInformation("Источник {NodeName} успешно обработан", sourceNode.Name);
            }
            else
            {
                logger.LogWarning("Источник {NodeName} не удалось обработать", sourceNode.Name);
            }
        }
        
        foreach (var targetNode in pipelineMessage.Targets)
        {
            logger.LogInformation("Обработка таргета: {NodeName} ({NodeType})", 
                targetNode.Name, targetNode.DataNodeType);
                
            var target = await ProcessDataNode(targetNode, cancellationToken);
            if (target != null)
            {
                request.Targets.Add(target);
                logger.LogInformation("Таргет {NodeName} успешно обработан", targetNode.Name);
            }
            else
            {
                logger.LogWarning("Таргет {NodeName} не удалось обработать", targetNode.Name);
            }
        }

        logger.LogInformation("Генерация схемы завершена для пайплайна {PipelineId}. Обработано источников: {SourceCount}, таргетов: {TargetCount}", 
            pipelineMessage.PipelineId, request.Sources.Count, request.Targets.Count);

        return request;
    }

    private async Task<SourceDto?> ProcessDataNode(DataNodeDto dataNode, CancellationToken cancellationToken = default)
    {
        logger.LogDebug("Начало обработки узла данных: {NodeName} ({NodeType})", dataNode.Name, dataNode.DataNodeType);
        
        if (dataNode.DataNodeType == DataNodeType.File && dataNode.FileConfig != null)
        {
            var fileUrl = dataNode.FileConfig.FileUrl;
            var fileType = dataNode.FileConfig.FileType;
            
            logger.LogDebug("Чтение файла из HDFS: {FileUrl}", fileUrl);
            await using var hdfsStream = await hadoopService.ReadFileAsync(fileUrl);

            SourceDto result = fileType switch
            {
                FileType.Csv => fileService.GetCsvSchema(fileUrl, hdfsStream),
                FileType.Json => fileService.GetJsonSchema(fileUrl, hdfsStream),
                FileType.Xml => fileService.GetXmlSchema(fileUrl, hdfsStream),
                _ => throw new NotSupportedException($"File type {fileType} is not supported")
            };

            logger.LogDebug("Схема файла {FileUrl} успешно получена", fileUrl);
            return result;
        }
        else if ((dataNode.DataNodeType == DataNodeType.PostgreSql || dataNode.DataNodeType == DataNodeType.ClickHouse) 
            && dataNode.DatabaseConfig != null)
        {
            var dbConfig = dataNode.DatabaseConfig;
            logger.LogDebug("Подключение к БД: {DatabaseType} {Host}:{Port}/{Database}", 
                dbConfig.DatabaseType, dbConfig.Host, dbConfig.Port, dbConfig.Database);
                
            var dbParams = new DatabaseParametersDto
            {
                Type = dbConfig.DatabaseType,
                Host = dbConfig.Host,
                Port = dbConfig.Port,
                Database = dbConfig.Database,
                User = dbConfig.Username,
                Password = dbConfig.Password,
                Schema = "public"
            };
            
            var result = dbConfig.DatabaseType switch
            {
                DatabaseType.PostgreSql => await databaseService.GetPostgreSqlSchemaAsync(dbParams),
                DatabaseType.ClickHouse => await databaseService.GetClickHouseSchemaAsync(dbParams),
                _ => throw new NotSupportedException($"Database type {dbConfig.DatabaseType} is not supported")
            };
            
            logger.LogDebug("Схема БД {DatabaseType} успешно получена", dbConfig.DatabaseType);
            return result;
        }
        
        logger.LogWarning("Неподдерживаемый тип узла данных: {NodeType}", dataNode.DataNodeType);
        return null;
    }
}