using System.Globalization;
using System.Xml;
using Core.Models.Files;

namespace DataProcessing.BLL.Analayzers;

internal class XmlSchemaAnalyzer
{
    public long RowCount { get; private set; }
    public List<FieldInfo> Fields { get; } = [];
    public Dictionary<string, long> PathOccurrences { get; } = new();
    private readonly Dictionary<string, FieldInfo> _fieldMap = new();
    private const int MaxSampleValues = 10;
    private string? _rootElementName;
    private readonly List<string> _pathList = [];

    public void Analyze(Stream stream)
    {
        using var reader = XmlReader.Create(stream, new XmlReaderSettings
        {
            IgnoreWhitespace = true,
            IgnoreComments = true,
            DtdProcessing = DtdProcessing.Ignore
        });

        while (reader.Read())
        {
            switch (reader.NodeType)
            {
                case XmlNodeType.Element:
                    if (_rootElementName == null)
                    {
                        _rootElementName = reader.Name;
                    }
                    else if (reader.Name == _rootElementName && reader.Depth == 1)
                    {
                        RowCount++;
                        _pathList.Clear();
                    }
                    else if (reader.Depth > 1)
                    {
                        var expectedPathLength = reader.Depth - 2;
                        while (_pathList.Count > expectedPathLength)
                        {
                            _pathList.RemoveAt(_pathList.Count - 1);
                        }
                        
                        _pathList.Add(reader.Name);
                        
                        var currentPath = string.Join(".", _pathList);
                        if (!PathOccurrences.ContainsKey(currentPath))
                            PathOccurrences[currentPath] = 0;
                        PathOccurrences[currentPath]++;

                        if (!reader.IsEmptyElement && reader.Read())
                        {
                            if (reader.NodeType == XmlNodeType.Text)
                            {
                                var value = reader.Value;
                                var name = _pathList[_pathList.Count - 1];
                                RecordValue(name, currentPath, value);
                            }
                        }
                    }
                    break;

                case XmlNodeType.EndElement:
                    if (_pathList.Count > 0 && reader.Depth > 1)
                    {
                        _pathList.RemoveAt(_pathList.Count - 1);
                    }
                    break;
            }
        }
    }

    private void RecordValue(string name, string path, string value)
    {
        if (!_fieldMap.TryGetValue(path, out var field))
        {
            field = new FieldInfo { Name = name, Path = path };
            _fieldMap[path] = field;
            Fields.Add(field);
        }

        field.TotalCount++;

        if (string.IsNullOrWhiteSpace(value))
        {
            field.NullCount++;
            return;
        }

        var dataType = "unknown";

        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
        {
            dataType = "integer";
        }
        else if (double.TryParse(value, NumberStyles.Float | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out _))
        {
            dataType = "float";
        }
        else if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
        {
            dataType = "date";
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
            case "integer" when dataType == "float":
                field.DataType = "float";
                break;
            case "date" when dataType == "string":
            case "integer" when dataType == "string":
            case "float" when dataType == "string":
                field.DataType = "string";
                break;
        }

        if (field.SampleValues.Count < MaxSampleValues && !field.SampleValues.Contains(value))
        {
            field.SampleValues.Add(value);
        }

        if (!field.UniqueValues.Contains(value))
        {
            field.UniqueValues.Add(value);
        }

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
            case "date":
            {
                if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateValue))
                {
                    if (!field.MinDate.HasValue || dateValue < field.MinDate.Value)
                        field.MinDate = dateValue;
                    if (!field.MaxDate.HasValue || dateValue > field.MaxDate.Value)
                        field.MaxDate = dateValue;
                }

                break;
            }
        }
    }

    public static string? GetMin(FieldInfo field)
    {
        switch (field.DataType)
        {
            case "integer" or "float":
                return field.MinNumeric?.ToString(CultureInfo.InvariantCulture);
            case "string":
                return field.MinStringLength?.ToString(CultureInfo.InvariantCulture);
            case "date":
                return field.MinDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            default:
                return null;
        }
    }

    public static string? GetMax(FieldInfo field)
    {
        switch (field.DataType)
        {
            case "integer" or "float":
                return field.MaxNumeric?.ToString(CultureInfo.InvariantCulture);
            case "string":
                return field.MaxStringLength?.ToString(CultureInfo.InvariantCulture);
            case "date":
                return field.MaxDate?.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            default:
                return null;
        }
    }

    public static string? GetAvg(FieldInfo field)
    {
        switch (field.DataType)
        {
            case "integer" or "float":
                return field.NumericCount > 0 ? (field.SumNumeric / field.NumericCount).ToString("F2", CultureInfo.InvariantCulture) : null;
            case "string":
                return field.StringCount > 0 ? ((double)field.SumStringLength / field.StringCount).ToString("F2", CultureInfo.InvariantCulture) : null;
            default:
                return null;
        }
    }
}
