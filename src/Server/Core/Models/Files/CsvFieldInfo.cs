namespace Core.Models.Files;

public class CsvFieldInfo
{
    public string Name { get; set; } = string.Empty;
    public string DataType { get; set; } = string.Empty;
    public long TotalCount { get; set; }
    public long NullCount { get; set; }
    public List<string> SampleValues { get; } = [];
    public HashSet<string> UniqueValues { get; } = [];
        
    public decimal? MinNumeric { get; set; }
    public decimal? MaxNumeric { get; set; }
    public decimal SumNumeric { get; set; }
    public long NumericCount { get; set; }
        
    public int? MinStringLength { get; set; }
    public int? MaxStringLength { get; set; }
    public long SumStringLength { get; set; }
    public long StringCount { get; set; }
        
    public DateTime? MinDate { get; set; }
    public DateTime? MaxDate { get; set; }
}
