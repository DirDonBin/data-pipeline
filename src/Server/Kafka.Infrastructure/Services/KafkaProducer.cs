using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;
using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.Infrastructure.Services;

public class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly KafkaSettings _settings;
    private readonly ILogger<KafkaProducer> _logger;
    private readonly IProducer<string, string> _producer;
    private readonly JsonSerializerOptions _jsonOptions;

    public KafkaProducer(IOptions<KafkaSettings> settings, ILogger<KafkaProducer> logger)
    {
        _settings = settings.Value;
        _logger = logger;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = _settings.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 30000,
            CompressionType = CompressionType.Snappy
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();

        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false
        };

        _logger.LogInformation("KafkaProducer инициализирован. Брокеры: {Brokers}", _settings.BootstrapServers);
    }

    public async Task SendMessageAsync<T>(string topic, string key, T message, CancellationToken cancellationToken = default) where T : class
    {
        try
        {
            var jsonMessage = JsonSerializer.Serialize(message, _jsonOptions);
            await SendMessageAsync(topic, key, jsonMessage, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка при сериализации сообщения для топика {Topic}, ключ: {Key}", topic, key);
            throw;
        }
    }

    public async Task SendMessageAsync(string topic, string key, string message, CancellationToken cancellationToken = default)
    {
        try
        {
            var kafkaMessage = new Message<string, string>
            {
                Key = key,
                Value = message
            };

            _logger.LogInformation("Отправка сообщения в Kafka топик {Topic}, ключ: {Key}", topic, key);

            var deliveryResult = await _producer.ProduceAsync(topic, kafkaMessage, cancellationToken);

            _logger.LogInformation("Сообщение успешно отправлено в топик {Topic}. Partition: {Partition}, Offset: {Offset}", 
                topic, deliveryResult.Partition.Value, deliveryResult.Offset.Value);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ошибка при отправке сообщения в Kafka. Топик: {Topic}, ключ: {Key}", topic, key);
            throw;
        }
    }

    public void Dispose()
    {
        _logger.LogInformation("Остановка KafkaProducer");
        _producer.Flush(TimeSpan.FromSeconds(10));
        _producer.Dispose();
        _logger.LogInformation("KafkaProducer остановлен");
    }
}
