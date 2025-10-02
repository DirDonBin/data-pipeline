using System.Buffers;
using System.Globalization;
using System.Text.Json;
using Core.Models;
using Core.Models.Files;

namespace DataProcessing.BLL.Analayzers;

internal class JsonSchemaAnalyzer
    {
        public long RowCount { get; private set; }
        public List<FieldInfo> Fields { get; } = [];
        public Dictionary<string, long> PathOccurrences { get; } = new();
        private readonly Dictionary<string, FieldInfo> _fieldMap = new();
        private const int MaxSampleValues = 100;

        private int _depth;
        private readonly List<string> _pathList = [];
        private readonly Stack<bool> _containerStack = new();
        private bool _isInArray;
        private int _arrayDepth;

        public void Analyze(Stream stream)
        {
            const int bufferSize = 65536;
            var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
            
            try
            {
                var bytesInBuffer = 0;
                var isFinalBlock = false;
                var state = new JsonReaderState(new JsonReaderOptions
                {
                    AllowTrailingCommas = true,
                    CommentHandling = JsonCommentHandling.Skip
                });

                while (!isFinalBlock)
                {
                    var bytesRead = stream.Read(buffer, bytesInBuffer, buffer.Length - bytesInBuffer);
                    
                    if (bytesRead == 0)
                    {
                        isFinalBlock = true;
                    }

                    bytesInBuffer += bytesRead;

                    var readOnlySpan = new ReadOnlySpan<byte>(buffer, 0, bytesInBuffer);
                    var reader = new Utf8JsonReader(readOnlySpan, isFinalBlock, state);

                    ProcessJson(ref reader);

                    state = reader.CurrentState;


                    var bytesConsumed = (int)reader.BytesConsumed;
                    var bytesRemaining = bytesInBuffer - bytesConsumed;
                    
                    if (bytesRemaining > 0)
                    {
                        buffer.AsSpan(bytesConsumed, bytesRemaining).CopyTo(buffer.AsSpan(0, bytesRemaining));
                    }
                    
                    bytesInBuffer = bytesRemaining;
                }

                if (!_isInArray && RowCount == 0)
                    RowCount = 1;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private void ProcessJson(ref Utf8JsonReader reader)
        {
            while (reader.Read())
            {
                switch (reader.TokenType)
                {
                    case JsonTokenType.StartArray:
                        if (_depth == 0)
                        {
                            _isInArray = true;
                            _arrayDepth = _depth;
                        }
                        _containerStack.Push(true);
                        _depth++;
                        break;

                    case JsonTokenType.EndArray:
                        _depth--;
                        _containerStack.Pop();
                        if (_depth == _arrayDepth && _isInArray)
                        {
                            _isInArray = false;
                        }
                        if (_pathList.Count > 0)
                        {
                            _pathList.RemoveAt(_pathList.Count - 1);
                        }
                        break;

                    case JsonTokenType.StartObject:
                        if (_containerStack.Count == 1 && _containerStack.Peek() && _depth == 1)
                        {
                            RowCount++;
                            
                            _pathList.Clear();
                        }
                        
                        _containerStack.Push(false);
                        _depth++;
                        break;

                    case JsonTokenType.EndObject:
                        _depth--;
                        _containerStack.Pop();
                        
                        if (_pathList.Count > 0 && _depth > 1)
                        {
                            _pathList.RemoveAt(_pathList.Count - 1);
                        }
                        break;

                    case JsonTokenType.PropertyName:
                        var propertyName = reader.GetString() ?? string.Empty;
                        
                        var objectDepth = 0;
                        foreach (var isArray in _containerStack)
                        {
                            if (!isArray) objectDepth++;
                        }
                        
                        var expectedPathLength = objectDepth - 1;
                        if (expectedPathLength < 0) expectedPathLength = 0;
                        
                        while (_pathList.Count > expectedPathLength)
                        {
                            _pathList.RemoveAt(_pathList.Count - 1);
                        }
                        
                        _pathList.Add(propertyName);
                        
                        var currentPath = string.Join(".", _pathList);
                        if (!PathOccurrences.ContainsKey(currentPath))
                            PathOccurrences[currentPath] = 0;
                        PathOccurrences[currentPath]++;
                        
                        break;

                    case JsonTokenType.String:
                    case JsonTokenType.Number:
                    case JsonTokenType.True:
                    case JsonTokenType.False:
                    case JsonTokenType.Null:
                        if (_pathList.Count > 0)
                        {
                            var path = string.Join(".", _pathList);
                            var name = _pathList[_pathList.Count - 1];
                            RecordValue(name, path, ref reader);
                        }
                        break;
                }
            }
        }

        private void RecordValue(string name, string path, ref Utf8JsonReader reader)
        {
            if (!_fieldMap.TryGetValue(path, out var field))
            {
                field = new FieldInfo { Name = name, Path = path };
                _fieldMap[path] = field;
                Fields.Add(field);
            }

            field.TotalCount++;

            if (reader.TokenType == JsonTokenType.Null)
            {
                field.NullCount++;
                return;
            }

            string? value = null;
            var dataType = "unknown";
            
            switch (reader.TokenType)
            {
                case JsonTokenType.String:
                {
                    value = reader.GetString();
                
                    if (!string.IsNullOrEmpty(value))
                    {
                        if (long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
                        {
                            dataType = "integer";
                        }
                        else if (double.TryParse(value, NumberStyles.Float | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out _))
                        {
                            dataType = "float";
                        }
                        else if (DateTime.TryParseExact(value, ["yyyy-MM-dd", "yyyy-MM-ddTHH:mm:ss", "yyyy-MM-ddTHH:mm:ss.fff", "yyyy-MM-ddTHH:mm:ss.fffzzz"
                                     ], 
                                     CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
                        {
                            dataType = "date";
                        }
                        else
                        {
                            dataType = "string";
                        }
                    }
                    else
                    {
                        dataType = "string";
                    }

                    break;
                }
                case JsonTokenType.Number:
                    try
                    {
                        var intValue = reader.GetInt64();
                        dataType = "integer";
                        value = intValue.ToString(CultureInfo.InvariantCulture);
                    }
                    catch
                    {
                        try
                        {
                            var doubleValue = reader.GetDouble();
                            dataType = "float";
                            value = doubleValue.ToString(CultureInfo.InvariantCulture);
                        }
                        catch
                        {
                            var decimalValue = reader.GetDecimal();
                            if (decimalValue % 1 == 0)
                            {
                                dataType = "integer";
                            }
                            else
                            {
                                dataType = "float";
                            }
                            value = decimalValue.ToString(CultureInfo.InvariantCulture);
                        }
                    }

                    break;
                case JsonTokenType.True or JsonTokenType.False:
                    dataType = "boolean";
                    value = reader.TokenType == JsonTokenType.True ? "true" : "false";
                    break;
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

            if (value == null) return;

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
                    if (DateTime.TryParseExact(value, ["yyyy-MM-dd", "yyyy-MM-ddTHH:mm:ss", "yyyy-MM-ddTHH:mm:ss.fff", "yyyy-MM-ddTHH:mm:ss.fffzzz"
                            ], 
                            CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateValue))
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