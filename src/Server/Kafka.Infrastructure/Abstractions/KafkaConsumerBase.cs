using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.Infrastructure.Abstractions;

public abstract class KafkaConsumerBase<TConsumer> : IMessageConsumer where TConsumer : class
{
    private readonly IKafkaConsumer _kafkaConsumer;
    private readonly KafkaSettings _settings;
    protected readonly ILogger<TConsumer> Logger;
    private CancellationTokenSource? _cts;
    private Task? _consumerTask;

    public abstract string Topic { get; }
    public abstract string GroupId { get; }

    protected KafkaConsumerBase(
        IOptions<KafkaSettings> settings,
        ILogger<TConsumer> logger)
    {
        _settings = settings.Value;
        Logger = logger;
        _kafkaConsumer = new Services.KafkaConsumer(settings, Microsoft.Extensions.Logging.Abstractions.NullLogger<Services.KafkaConsumer>.Instance, GroupId);
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _consumerTask = Task.Run(async () =>
        {
            try
            {
                Logger.LogInformation("Запуск consumer {ConsumerType} для топика {Topic}, группа {GroupId}", 
                    typeof(TConsumer).Name, Topic, GroupId);

                await _kafkaConsumer.StartConsumingAsync(Topic, HandleMessageInternalAsync, _cts.Token);
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Критическая ошибка в consumer {ConsumerType}", typeof(TConsumer).Name);
            }
        }, _cts.Token);

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_cts == null || _consumerTask == null)
            return;

        Logger.LogInformation("Остановка consumer {ConsumerType}", typeof(TConsumer).Name);

        _cts.Cancel();

        try
        {
            await _consumerTask.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Ошибка при остановке consumer {ConsumerType}", typeof(TConsumer).Name);
        }
        finally
        {
            _cts.Dispose();
        }
    }

    private async Task HandleMessageInternalAsync(string key, string message)
    {
        try
        {
            Logger.LogInformation("Получено сообщение в {ConsumerType}. Key: {Key}", typeof(TConsumer).Name, key);
            await HandleMessageAsync(key, message);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Ошибка при обработке сообщения в {ConsumerType}. Key: {Key}", typeof(TConsumer).Name, key);
        }
    }

    protected abstract Task HandleMessageAsync(string key, string message);
}
