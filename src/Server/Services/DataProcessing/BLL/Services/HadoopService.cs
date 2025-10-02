using System.Net;
using Core.Enums;
using DataProcessing.BLL.Interfaces;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace DataProcessing.BLL.Services;

internal class HadoopService : IHadoopService
{
    private readonly HttpClient _httpClient;
    private readonly ILogger<HadoopService> _logger;
    private readonly string _webHdfsUrl;
    private readonly string _csvDataPath;
    private readonly string _jsonDataPath;
    private readonly string _xmlDataPath;

    public HadoopService(HttpClient httpClient, IConfiguration configuration, ILogger<HadoopService> logger)
    {
        _httpClient = httpClient;
        _logger = logger;
        _webHdfsUrl = configuration["Hadoop:WebHdfsUrl"] 
            ?? throw new InvalidOperationException("Hadoop:WebHdfsUrl is not configured");
        _csvDataPath = configuration["Hadoop:CsvDataPath"] 
            ?? throw new InvalidOperationException("Hadoop:CsvDataPath is not configured");
        _jsonDataPath = configuration["Hadoop:JsonDataPath"] 
            ?? throw new InvalidOperationException("Hadoop:JsonDataPath is not configured");
        _xmlDataPath = configuration["Hadoop:XmlDataPath"] 
            ?? throw new InvalidOperationException("Hadoop:XmlDataPath is not configured");
        
        _httpClient.Timeout = TimeSpan.FromMinutes(5);
    }

    public async Task<string> UploadFileAsync(Stream file, string fileName, FileType fileType)
    {
        try
        {
            var uploadDirectory = fileType switch
            {
                FileType.Csv => _csvDataPath,
                FileType.Json => _jsonDataPath,
                FileType.Xml => _xmlDataPath,
                _ => _csvDataPath
            };
            
            var uniqueId = Guid.CreateVersion7();
            var hdfsPath = $"{uploadDirectory}/{uniqueId}/{fileName}";
            
            _logger.LogInformation("Uploading file {FileName} to HDFS path {HdfsPath}", fileName, hdfsPath);

            var createUrl = $"{_webHdfsUrl}{hdfsPath}?op=CREATE&overwrite=true";
            var createRequest = new HttpRequestMessage(HttpMethod.Put, createUrl);
            createRequest.Content = new StringContent("");
            
            var createResponse = await _httpClient.SendAsync(createRequest, HttpCompletionOption.ResponseHeadersRead);
            
            if (createResponse.StatusCode != HttpStatusCode.TemporaryRedirect)
            {
                var errorContent = await createResponse.Content.ReadAsStringAsync();
                _logger.LogError("Failed to create file in HDFS. Status: {Status}, Response: {Response}", 
                    createResponse.StatusCode, errorContent);
                throw new Exception($"Failed to create file in HDFS: {createResponse.StatusCode}");
            }

            var uploadUrl = createResponse.Headers.Location?.ToString();
            if (string.IsNullOrEmpty(uploadUrl))
            {
                throw new Exception("Failed to get upload URL from HDFS");
            }

            _logger.LogInformation("Got upload URL: {UploadUrl}", uploadUrl);
            
            uploadUrl = ReplaceDataNodeHostname(uploadUrl);
            _logger.LogInformation("Adjusted upload URL: {UploadUrl}", uploadUrl);
            var uploadRequest = new HttpRequestMessage(HttpMethod.Put, uploadUrl);
            
            uploadRequest.Content = new StreamContent(file);
            uploadRequest.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");

            var uploadResponse = await _httpClient.SendAsync(uploadRequest);
            
            if (!uploadResponse.IsSuccessStatusCode)
            {
                var errorContent = await uploadResponse.Content.ReadAsStringAsync();
                _logger.LogError("Failed to upload file data to HDFS. Status: {Status}, Response: {Response}", 
                    uploadResponse.StatusCode, errorContent);
                throw new Exception($"Failed to upload file data to HDFS: {uploadResponse.StatusCode}");
            }

            _logger.LogInformation("Successfully uploaded file {FileName} to HDFS", fileName);

            var fileUrl = $"{_webHdfsUrl}{hdfsPath}";
            return fileUrl;
        }
        catch (HttpRequestException ex)
        {
            _logger.LogError(ex, "HTTP error uploading file {FileName} to HDFS. Check if Hadoop is running and WebHDFS is enabled at {WebHdfsUrl}", fileName, _webHdfsUrl);
            throw new Exception($"Failed to connect to Hadoop at {_webHdfsUrl}. Ensure Hadoop is running and WebHDFS is enabled.", ex);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error uploading file {FileName} to HDFS", fileName);
            throw;
        }
    }

    public async Task<Stream> ReadFileAsync(string fileUrl)
    {
        try
        {
            _logger.LogInformation("Reading file from HDFS: {FileUrl}", fileUrl);

            var readUrl = $"{fileUrl}?op=OPEN";
            var readRequest = new HttpRequestMessage(HttpMethod.Get, readUrl);
            
            var response = await _httpClient.SendAsync(readRequest, HttpCompletionOption.ResponseHeadersRead);

            if (response.StatusCode == HttpStatusCode.TemporaryRedirect)
            {
                var downloadUrl = response.Headers.Location?.ToString();
                if (string.IsNullOrEmpty(downloadUrl))
                {
                    throw new Exception("Failed to get download URL from HDFS");
                }

                _logger.LogInformation("Got download URL: {DownloadUrl}", downloadUrl);
                
                downloadUrl = ReplaceDataNodeHostname(downloadUrl);
                _logger.LogInformation("Adjusted download URL: {DownloadUrl}", downloadUrl);
                
                var downloadRequest = new HttpRequestMessage(HttpMethod.Get, downloadUrl);
                response = await _httpClient.SendAsync(downloadRequest, HttpCompletionOption.ResponseHeadersRead);
            }

            if (!response.IsSuccessStatusCode)
            {
                var errorContent = await response.Content.ReadAsStringAsync();
                _logger.LogError("Failed to read file from HDFS. Status: {Status}, Response: {Response}", 
                    response.StatusCode, errorContent);
                throw new Exception($"Failed to read file from HDFS: {response.StatusCode}");
            }

            _logger.LogInformation("Successfully read file from HDFS");

            return await response.Content.ReadAsStreamAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error reading file from HDFS: {FileUrl}", fileUrl);
            throw;
        }
    }

    public async Task<bool> FileExistsAsync(string fileUrl)
    {
        try
        {
            _logger.LogInformation("Checking if file exists in HDFS: {FileUrl}", fileUrl);

            var statusUrl = $"{fileUrl}?op=GETFILESTATUS";

            var response = await _httpClient.GetAsync(statusUrl);

            var exists = response.IsSuccessStatusCode;
            _logger.LogInformation("File exists check result: {Exists}", exists);

            return exists;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking file existence in HDFS: {FileUrl}", fileUrl);
            return false;
        }
    }

    private static string ReplaceDataNodeHostname(string url)
    {
        if (string.IsNullOrEmpty(url))
            return url;

        var uri = new Uri(url);
        
        if (uri.Host != "localhost" && uri.Host != "127.0.0.1")
        {
            var builder = new UriBuilder(uri)
            {
                Host = "localhost"
            };
            return builder.Uri.ToString();
        }

        return url;
    }
}
