using DataPipeline.ApiClient.Internal;

namespace WebClient.Models
{
    public class ConnectonSourceDto
    {
        public Guid Id { get; set; } = Guid.NewGuid();
        public string Type { get; set; } = "";
        public string? Host { get; set; }
        public int Port { get; set; } = 0;
        public string? Login { get; set; }
        public string? Password { get; set; }
        public string? DataBase { get; set; }
        public string? Error { get; set; }
        public string? FileUrl { get; set; }
        public FileType FileType { get; set; } = FileType.Unknown;
    }
}
