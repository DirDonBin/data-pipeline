using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.Interfaces;
using Microsoft.Extensions.Hosting;

namespace Kafka.Infrastructure.HostedServices;

internal class ConsumerHostedService : IHostedService
{
    private readonly IMessageConsumer _consumer;

    public ConsumerHostedService(IMessageConsumer consumer)
    {
        _consumer = consumer;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        return _consumer.StartAsync(cancellationToken);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return _consumer.StopAsync(cancellationToken);
    }
}