namespace Core.Enums;

public enum PipelineStatus
{
    Created,
    Generating,
    GenerateSuccess,
    GenerateError,
    Running,
    LastRunCompleted,
    LastRunError,
    Canceled
}
