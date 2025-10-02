namespace Kafka.Infrastructure.Interfaces;

public interface IMessageConsumer
{
    string Topic { get; }
    string GroupId { get; }
    Task StartAsync(CancellationToken cancellationToken);
    Task StopAsync(CancellationToken cancellationToken);
}
