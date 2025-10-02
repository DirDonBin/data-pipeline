using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Gateway.Middleware;

public class OpenApiTransformMiddleware
{
    private readonly RequestDelegate _next;
    private readonly IConfiguration _configuration;
    private readonly Dictionary<string, string> _routePrefixes;

    public OpenApiTransformMiddleware(RequestDelegate next, IConfiguration configuration)
    {
        _next = next;
        _configuration = configuration;
        _routePrefixes = ExtractRoutePrefixes();
    }

    private Dictionary<string, string> ExtractRoutePrefixes()
    {
        var prefixes = new Dictionary<string, string>();
        var routes = _configuration.GetSection("ReverseProxy:Routes").GetChildren();

        foreach (var route in routes)
        {
            var pathPattern = route["Match:Path"];
            var clusterId = route["ClusterId"];

            if (!string.IsNullOrEmpty(pathPattern) && !string.IsNullOrEmpty(clusterId))
            {
                var match = Regex.Match(pathPattern, @"^(\w+)/");
                if (match.Success)
                {
                    var prefix = match.Groups[1].Value;
                    prefixes[$"/{prefix}"] = clusterId;
                }
            }
        }

        return prefixes;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        var path = context.Request.Path.Value;
        
        if (path != null && path.EndsWith("/openapi/v1.json"))
        {
            string? matchedPrefix = null;
            string? clusterId = null;

            foreach (var (prefix, cluster) in _routePrefixes)
            {
                if (path.StartsWith($"{prefix}/"))
                {
                    matchedPrefix = prefix;
                    clusterId = cluster;
                    break;
                }
            }
            
            if (matchedPrefix != null && clusterId != null)
            {
                var originalBody = context.Response.Body;
                using var memoryStream = new MemoryStream();
                context.Response.Body = memoryStream;
                
                await _next(context);
                
                memoryStream.Seek(0, SeekOrigin.Begin);
                var responseBody = await new StreamReader(memoryStream).ReadToEndAsync();
                
                if (context.Response.StatusCode == 200 && !string.IsNullOrEmpty(responseBody))
                {
                    var openApiDoc = JsonNode.Parse(responseBody);
                    if (openApiDoc != null)
                    {
                        if (openApiDoc["servers"] == null)
                        {
                            openApiDoc["servers"] = new JsonArray();
                        }
                        
                        var servers = openApiDoc["servers"]!.AsArray();
                        servers.Clear();
                        servers.Add(new JsonObject
                        {
                            ["url"] = matchedPrefix,
                            ["description"] = $"Gateway ({FormatClusterName(clusterId)})"
                        });
                        
                        var modifiedBody = openApiDoc.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
                        var bytes = Encoding.UTF8.GetBytes(modifiedBody);
                        
                        context.Response.Body = originalBody;
                        context.Response.ContentLength = bytes.Length;
                        await context.Response.Body.WriteAsync(bytes);
                        return;
                    }
                }
                
                memoryStream.Seek(0, SeekOrigin.Begin);
                context.Response.Body = originalBody;
                await memoryStream.CopyToAsync(originalBody);
                return;
            }
        }
        
        await _next(context);
    }

    private static string FormatClusterName(string clusterId)
    {
        return clusterId switch
        {
            "data" => "Data Processing API",
            "internal" => "Internal API",
            _ => $"{char.ToUpper(clusterId[0])}{clusterId[1..]} API"
        };
    }
}

public static class OpenApiTransformMiddlewareExtensions
{
    public static IApplicationBuilder UseOpenApiTransform(this IApplicationBuilder builder)
    {
        return builder.UseMiddleware<OpenApiTransformMiddleware>();
    }
}
