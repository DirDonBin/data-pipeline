using System;
using System.Collections.Generic;
using System.Text;

namespace DataPipeline.ApiClient.Data
{
    public class FileParameter
    {
        public Stream Data { get; set; }
        public string FileName { get; set; }
        public string ContentType { get; set; }
        public string Name { get; set; }
    }
}
