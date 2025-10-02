using Core.Enums;
using Core.Models.Kafka;
using Core.Models.Pipelines;
using Internal.BLL.Interfaces;
using Internal.BLL.Producers;
using Internal.DAL;
using Internal.DAL.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;

namespace Internal.BLL.Services;

internal class PipelineService(
    PipelineDbContext context,
    PipelineStartProducer pipelineStartProducer,
    ILogger<PipelineService> logger) : IPipelineService
{
    public async Task<Guid> CreatePipelineAsync(
        CreatePipelineDto dto,
        Guid userId,
        CancellationToken cancellationToken = default)
    {
        var pipelineId = Guid.CreateVersion7();
        var now = DateTime.UtcNow;

        await using var transaction = await context.Database.BeginTransactionAsync(cancellationToken);

        try
        {
            var pipeline = new Pipeline
            {
                Id = pipelineId,
                Name = dto.Name,
                Description = dto.Description,
                CreatedUserId = userId,
                CreatedAt = now,
                IsActive = true,
                Status = PipelineStatus.Created
            };

            context.Pipelines.Add(pipeline);

            foreach (var sourceDto in dto.Sources)
            {
                var sourceNode = CreateDataNode(sourceDto, pipelineId, NodeType.Source, now);
                context.PipelineDataNodes.Add(sourceNode);

                if (sourceDto.FileConfig != null)
                {
                    var fileConfig = CreateFileConfig(sourceDto.FileConfig, sourceNode.Id, now);
                    context.FileNodeConfigs.Add(fileConfig);
                }

                if (sourceDto.DatabaseConfig != null)
                {
                    var dbConfig = CreateDatabaseConfig(sourceDto.DatabaseConfig, sourceNode.Id, now);
                    context.DatabaseNodeConfigs.Add(dbConfig);
                }
            }

            foreach (var targetDto in dto.Targets)
            {
                var targetNode = CreateDataNode(targetDto, pipelineId, NodeType.Target, now);
                context.PipelineDataNodes.Add(targetNode);

                if (targetDto.FileConfig != null)
                {
                    var fileConfig = CreateFileConfig(targetDto.FileConfig, targetNode.Id, now);
                    context.FileNodeConfigs.Add(fileConfig);
                }

                if (targetDto.DatabaseConfig != null)
                {
                    var dbConfig = CreateDatabaseConfig(targetDto.DatabaseConfig, targetNode.Id, now);
                    context.DatabaseNodeConfigs.Add(dbConfig);
                }
            }

            await context.SaveChangesAsync(cancellationToken);
            await transaction.CommitAsync(cancellationToken);

            logger.LogInformation("Пайплайн {PipelineId} успешно создан пользователем {UserId}", pipelineId, userId);
            
            var kafkaMessage = new PipelineStartMessage
            {
                RequestUid = Guid.NewGuid(),
                PipelineId = pipelineId,
                PipelineName = dto.Name,
                Sources = dto.Sources,
                Targets = dto.Targets
            };

            try
            {
                await pipelineStartProducer.SendAsync(kafkaMessage.RequestUid.ToString(), kafkaMessage, cancellationToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Ошибка при отправке сообщения в Kafka для пайплайна {PipelineId}", pipelineId);
            }

            return pipelineId;
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(cancellationToken);
            logger.LogError(ex, "Ошибка при создании пайплайна");
            throw;
        }
    }

    public async Task ChangePipelineStatus(Guid uid, PipelineStatus status)
    {
        await context.Pipelines
            .Where(x => x.Id == uid)
            .ExecuteUpdateAsync(x => x.SetProperty(y => y.Status, status));
    }
    
    private static PipelineDataNode CreateDataNode(DataNodeDto dto, Guid pipelineId, NodeType nodeType, DateTime createdAt)
    {
        return new PipelineDataNode
        {
            Id = Guid.CreateVersion7(),
            PipelineId = pipelineId,
            Name = dto.Name,
            NodeType = nodeType,
            DataNodeType = dto.DataNodeType,
            CreatedAt = createdAt
        };
    }

    private static FileNodeConfig CreateFileConfig(FileNodeConfigDto dto, Guid dataNodeId, DateTime createdAt)
    {
        return new FileNodeConfig
        {
            Id = Guid.CreateVersion7(),
            DataNodeId = dataNodeId,
            FileUrl = dto.FileUrl,
            FileType = dto.FileType,
            CreatedAt = createdAt
        };
    }

    private static DatabaseNodeConfig CreateDatabaseConfig(DatabaseNodeConfigDto dto, Guid dataNodeId, DateTime createdAt)
    {
        return new DatabaseNodeConfig
        {
            Id = Guid.CreateVersion7(),
            DataNodeId = dataNodeId,
            DatabaseType = dto.DatabaseType,
            Host = dto.Host,
            Port = dto.Port,
            Database = dto.Database,
            Username = dto.Username,
            Password = dto.Password,
            AdditionalParams = dto.AdditionalParams,
            CreatedAt = createdAt
        };
    }
}
