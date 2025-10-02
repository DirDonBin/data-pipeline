namespace Kafka.Infrastructure.Interfaces;

public interface IMessageProducer<in TMessage> where TMessage : class
{
    Task SendAsync(string key, TMessage message, CancellationToken cancellationToken = default);
}
