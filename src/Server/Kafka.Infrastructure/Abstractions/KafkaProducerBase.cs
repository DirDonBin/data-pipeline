using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kafka.Infrastructure.Abstractions;

public abstract class KafkaProducerBase<TMessage, TProducer> : IMessageProducer<TMessage> 
    where TMessage : class 
    where TProducer : class
{
    private readonly IKafkaProducer _kafkaProducer;
    private readonly KafkaSettings _settings;
    protected readonly ILogger<TProducer> Logger;

    public abstract string Topic { get; }

    protected KafkaProducerBase(
        IKafkaProducer kafkaProducer,
        IOptions<KafkaSettings> settings,
        ILogger<TProducer> logger)
    {
        _kafkaProducer = kafkaProducer;
        _settings = settings.Value;
        Logger = logger;
    }

    public async Task SendAsync(string key, TMessage message, CancellationToken cancellationToken = default)
    {
        try
        {
            Logger.LogInformation("Отправка сообщения в топик {Topic} через {ProducerType}. Key: {Key}", 
                Topic, typeof(TProducer).Name, key);

            await _kafkaProducer.SendMessageAsync(Topic, key, message, cancellationToken);

            Logger.LogInformation("Сообщение успешно отправлено в топик {Topic}. Key: {Key}", Topic, key);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Ошибка при отправке сообщения в топик {Topic} через {ProducerType}. Key: {Key}", 
                Topic, typeof(TProducer).Name, key);
            throw;
        }
    }
}
