namespace Kafka.Infrastructure.Settings;

public class KafkaSettings
{
    public required string BootstrapServers { get; init; }
    public Dictionary<string, string> Topics { get; init; } = new();
}
