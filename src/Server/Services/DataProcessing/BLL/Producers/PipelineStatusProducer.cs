using Core.Models;
using Core.Models.Kafka;
using Kafka.Infrastructure.Abstractions;
using Kafka.Infrastructure.Interfaces;
using Kafka.Infrastructure.Settings;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace DataProcessing.BLL.Producers;

public class PipelineStatusProducer : KafkaProducerBase<PipelineStatusMessage, PipelineStatusProducer>
{
    public override string Topic => "pipeline_status_update";

    public PipelineStatusProducer(
        IKafkaProducer kafkaProducer,
        IOptions<KafkaSettings> settings,
        ILogger<PipelineStatusProducer> logger) 
        : base(kafkaProducer, settings, logger)
    {
    }
}
