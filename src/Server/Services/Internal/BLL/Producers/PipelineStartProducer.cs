using Core.Models;
using Core.Models.Kafka;
using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Internal.BLL.Producers;

public class PipelineStartProducer : KafkaProducerBase<PipelineStartMessage, PipelineStartProducer>
{
    public override string Topic => "data_processing_start_generate";

    public PipelineStartProducer(
        IKafkaProducer kafkaProducer,
        IOptions<KafkaSettings> settings,
        ILogger<PipelineStartProducer> logger) 
        : base(kafkaProducer, settings, logger)
    {
    }
}
