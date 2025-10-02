namespace Kafka.Infrastructure.Interfaces;

public interface IKafkaProducer
{
    Task SendMessageAsync<T>(string topic, string key, T message, CancellationToken cancellationToken = default) where T : class;
    Task SendMessageAsync(string topic, string key, string message, CancellationToken cancellationToken = default);
}
