using Core.Models;
using Core.Models.Kafka;
using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataProcessing.BLL.Producers;

public class AiRequestProducer : KafkaProducerBase<GenerateAiRequest, AiRequestProducer>
{
    public override string Topic => "ai_generate_request";

    public AiRequestProducer(
        IKafkaProducer kafkaProducer,
        IOptions<KafkaSettings> settings,
        ILogger<AiRequestProducer> logger) 
        : base(kafkaProducer, settings, logger)
    {
    }
}
