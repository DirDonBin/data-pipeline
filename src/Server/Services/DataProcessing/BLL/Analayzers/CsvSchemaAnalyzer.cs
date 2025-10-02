using System.Globalization;
using System.Text;
using Core.Models;
using Core.Models.Files;

namespace DataProcessing.BLL.Analayzers;

internal class CsvSchemaAnalyzer
{
    public long RowCount { get; private set; }
    public List<CsvFieldInfo> Fields { get; } = [];
    public char Delimiter { get; private set; }
    private const int MaxSampleValues = 10;

    public void AnalyzeWithAutoDelimiter(Stream stream)
    {
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 8192, leaveOpen: true);
        
        var headerLine = reader.ReadLine();
        if (string.IsNullOrEmpty(headerLine))
            return;

        Delimiter = DetectDelimiter(headerLine);
        var headers = ParseCsvLine(headerLine, Delimiter);
        
        foreach (var t in headers)
        {
            var field = new CsvFieldInfo 
            { 
                Name = t
            };
            Fields.Add(field);
        }

        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (string.IsNullOrEmpty(line))
                continue;

            RowCount++;
            
            var values = ParseCsvLine(line, Delimiter);
            
            for (var i = 0; i < Math.Min(values.Length, headers.Length); i++)
            {
                var field = Fields[i];
                RecordValue(field, values[i]);
            }
        }
    }

    public void Analyze(Stream stream, char delimiter)
    {
        Delimiter = delimiter;
        
        using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 8192, leaveOpen: true);
        
        var headerLine = reader.ReadLine();
        if (string.IsNullOrEmpty(headerLine))
            return;

        var headers = ParseCsvLine(headerLine, delimiter);
        
        foreach (var t in headers)
        {
            var field = new CsvFieldInfo 
            { 
                Name = t
            };
            Fields.Add(field);
        }

        while (!reader.EndOfStream)
        {
            var line = reader.ReadLine();
            if (string.IsNullOrEmpty(line))
                continue;

            RowCount++;
            
            var values = ParseCsvLine(line, delimiter);
            
            for (var i = 0; i < Math.Min(values.Length, headers.Length); i++)
            {
                var field = Fields[i];
                RecordValue(field, values[i]);
            }
        }
    }

    // ReSharper disable once CognitiveComplexity
    private static string[] ParseCsvLine(string line, char delimiter)
    {
        var values = new List<string>();
        var currentValue = new StringBuilder();
        var inQuotes = false;

        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];

            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    currentValue.Append('"');
                    i++;
                }
                else
                {
                    inQuotes = !inQuotes;
                }
            }
            else if (c == delimiter && !inQuotes)
            {
                values.Add(currentValue.ToString());
                currentValue.Clear();
            }
            else
            {
                currentValue.Append(c);
            }
        }

        values.Add(currentValue.ToString());
        return values.ToArray();
    }

    // ReSharper disable once CognitiveComplexity
    private static void RecordValue(CsvFieldInfo field, string value)
    {
        field.TotalCount++;

        if (string.IsNullOrWhiteSpace(value))
        {
            field.NullCount++;
            return;
        }

        string dataType;

        if (bool.TryParse(value, out _))
        {
            dataType = "boolean";
        }
        else if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
        {
            dataType = "integer";
        }
        else if (double.TryParse(value, NumberStyles.Float | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out _))
        {
            dataType = "float";
        }
        else if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind, out _) ||
                 DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out _))
        {
            dataType = "datetime";
        }
        else
        {
            dataType = "string";
        }

        if (string.IsNullOrEmpty(field.DataType))
        {
            field.DataType = dataType;
        }
        else switch (field.DataType)
        {
            case "boolean" when dataType != "boolean":
                field.DataType = dataType;
                break;
            case "integer" when dataType == "float":
                field.DataType = "float";
                break;
            case "integer" when dataType == "datetime":
            case "float" when dataType == "datetime":
            case "datetime" when dataType is "integer" or "float":
                field.DataType = "string";
                break;
            default:
            {
                if (field.DataType != "string" && dataType == "string")
                {
                    field.DataType = "string";
                }

                break;
            }
        }

        if (field.SampleValues.Count < MaxSampleValues && !field.SampleValues.Contains(value))
        {
            field.SampleValues.Add(value);
        }

        field.UniqueValues.Add(value);

        switch (field.DataType)
        {
            case "integer" or "float":
            {
                if (decimal.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var numValue))
                {
                    if (!field.MinNumeric.HasValue || numValue < field.MinNumeric.Value)
                        field.MinNumeric = numValue;
                    if (!field.MaxNumeric.HasValue || numValue > field.MaxNumeric.Value)
                        field.MaxNumeric = numValue;
                    field.SumNumeric += numValue;
                    field.NumericCount++;
                }

                break;
            }
            case "string":
            {
                var length = value.Length;
                if (!field.MinStringLength.HasValue || length < field.MinStringLength.Value)
                    field.MinStringLength = length;
                if (!field.MaxStringLength.HasValue || length > field.MaxStringLength.Value)
                    field.MaxStringLength = length;
                field.SumStringLength += length;
                field.StringCount++;
                break;
            }
            case "datetime":
            {
                if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind, out var dateValue))
                {
                    if (!field.MinDate.HasValue || dateValue < field.MinDate.Value)
                        field.MinDate = dateValue;
                    if (!field.MaxDate.HasValue || dateValue > field.MaxDate.Value)
                        field.MaxDate = dateValue;
                }
                else if (DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var dateOffsetValue))
                {
                    dateValue = dateOffsetValue.DateTime;
                    if (!field.MinDate.HasValue || dateValue < field.MinDate.Value)
                        field.MinDate = dateValue;
                    if (!field.MaxDate.HasValue || dateValue > field.MaxDate.Value)
                        field.MaxDate = dateValue;
                }

                break;
            }
        }
    }

    public static string? GetMin(CsvFieldInfo field)
    {
        switch (field.DataType)
        {
            case "integer" or "float":
                return field.MinNumeric?.ToString(CultureInfo.InvariantCulture);
            case "string":
                return field.MinStringLength?.ToString(CultureInfo.InvariantCulture);
            case "datetime":
                return field.MinDate?.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);
            default:
                return null;
        }
    }

    public static string? GetMax(CsvFieldInfo field)
    {
        return field.DataType switch
        {
            "integer" or "float" => field.MaxNumeric?.ToString(CultureInfo.InvariantCulture),
            "string" => field.MaxStringLength?.ToString(CultureInfo.InvariantCulture),
            "datetime" => field.MaxDate?.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture),
            _ => null
        };
    }

    public static string? GetAvg(CsvFieldInfo field)
    {
        return field.DataType switch
        {
            "integer" or "float" => field.NumericCount > 0
                ? (field.SumNumeric / field.NumericCount).ToString("F2", CultureInfo.InvariantCulture)
                : null,
            "string" => field.StringCount > 0
                ? ((double)field.SumStringLength / field.StringCount).ToString("F2", CultureInfo.InvariantCulture)
                : null,
            _ => null
        };
    }

    private static char DetectDelimiter(string line)
    {
        var separators = new[] { ',', ';', '\t' };
        var counts = separators.Select(s => (separator: s, count: line.Count(c => c == s))).ToList();
        return counts.OrderByDescending(x => x.count).First().separator;
    }
}
