namespace Kafka.Infrastructure.Interfaces;

public interface IKafkaConsumer
{
    Task StartConsumingAsync(string topic, Func<string, string, Task> messageHandler, CancellationToken cancellationToken);
}
