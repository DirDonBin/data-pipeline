using Gateway.Middleware;
using Scalar.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(serverOptions =>
{
    serverOptions.Limits.MaxRequestBodySize = 107_374_182_400; // 100 GB
});

builder.Services.AddOpenApi();

var conf = builder.Configuration.GetSection("ReverseProxy");
builder.Services.AddReverseProxy()
    .LoadFromConfig(conf);

var app = builder.Build();

app.MapOpenApi();
app.UseRouting();
app.UseOpenApiTransform();
app.MapReverseProxy();

app.MapScalarApiReference(options =>
{
    options.AddDocument("data-v1", "Data Processing API", "/data/openapi/v1.json");
    options.AddDocument("internal-v1", "Internal API", "/internal/openapi/v1.json");
});

app.UseHttpsRedirection();

app.Run();
