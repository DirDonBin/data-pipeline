using Confluent.Kafka;
using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.Infrastructure.Services;

public class KafkaConsumer : IKafkaConsumer, IDisposable
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaConsumer> _logger;
    private readonly string _groupId;
    private IConsumer<string, string>? _consumer;

    public KafkaConsumer(IOptions<KafkaSettings> settings, ILogger<KafkaConsumer> logger, string groupId)
    {
        _settings = settings.Value;
        _logger = logger;
        _groupId = groupId;
    }

    public async Task StartConsumingAsync(string topic, Func<string, string, Task> messageHandler, CancellationToken cancellationToken)
    {
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            GroupId = _groupId,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false
        };

        _consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        _consumer.Subscribe(topic);

        _logger.LogInformation("KafkaConsumer запущен для топика {Topic}, группа: {GroupId}", topic, _groupId);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);

                    if (consumeResult?.Message?.Value == null) continue;

                    _logger.LogInformation("Получено сообщение из топика {Topic}, ключ: {Key}", topic, consumeResult.Message.Key);

                    await messageHandler(consumeResult.Message.Key, consumeResult.Message.Value);

                    _consumer.StoreOffset(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Ошибка при чтении сообщения из Kafka");
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Ошибка при обработке сообщения из Kafka");
                }
            }
        }
        finally
        {
            _consumer.Close();
            _logger.LogInformation("KafkaConsumer остановлен для топика {Topic}", topic);
        }
    }

    public void Dispose()
    {
        _consumer?.Dispose();
    }
}
