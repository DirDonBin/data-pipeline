#!/usr/bin/env pwsh

param(
    [switch]$SkipInstall,
    [string]$ConfigFile = "api-sources.json"
)

$ErrorActionPreference = "Stop"

Write-Host "=== API Client Generation ===" -ForegroundColor Cyan
Write-Host ""

# Check NSwag installation
if (-not $SkipInstall) {
    Write-Host "Checking NSwag installation..." -ForegroundColor Yellow
    $installed = dotnet tool list -g | Select-String "nswag.consolecore"
    
    if (-not $installed) {
        Write-Host "Installing NSwag..." -ForegroundColor Yellow
        dotnet tool install -g NSwag.ConsoleCore
        if ($LASTEXITCODE -ne 0) {
            Write-Error "Failed to install NSwag"
            exit 1
        }
        Write-Host "NSwag installed successfully!" -ForegroundColor Green
    } else {
        Write-Host "NSwag already installed" -ForegroundColor Green
    }
}

# Load configuration
$configPath = Join-Path $PSScriptRoot $ConfigFile
if (-not (Test-Path $configPath)) {
    Write-Error "Configuration file not found: $configPath"
    exit 1
}

Write-Host "Loading configuration from: $ConfigFile" -ForegroundColor Gray
$config = Get-Content $configPath | ConvertFrom-Json

# Create Generated directory
$genDir = Join-Path $PSScriptRoot "Generated"
if (-not (Test-Path $genDir)) {
    New-Item -ItemType Directory -Path $genDir | Out-Null
    Write-Host "Created directory: Generated/" -ForegroundColor Green
}

# Load template
$templatePath = Join-Path $PSScriptRoot "nswag-template.json"
if (-not (Test-Path $templatePath)) {
    Write-Error "Template file not found: $templatePath"
    exit 1
}

$template = Get-Content $templatePath -Raw

Write-Host ""
Write-Host "Gateway Base URL: $($config.gatewayBaseUrl)" -ForegroundColor Cyan
Write-Host "APIs to generate: $($config.apis.Count)" -ForegroundColor Cyan
Write-Host ""

$successCount = 0
$failCount = 0

# Generate clients for each API
foreach ($api in $config.apis) {
    Write-Host "--- Generating $($api.displayName) ---" -ForegroundColor Cyan
    Write-Host "  Name: $($api.name)" -ForegroundColor Gray
    Write-Host "  URL: $($config.gatewayBaseUrl)$($api.openApiPath)" -ForegroundColor Gray
    Write-Host "  Namespace: $($api.namespace)" -ForegroundColor Gray
    Write-Host "  Output: $($api.outputFile)" -ForegroundColor Gray
    
    # Replace placeholders in template
    $configContent = $template `
        -replace '\{URL\}', "$($config.gatewayBaseUrl)$($api.openApiPath)" `
        -replace '\{NAMESPACE\}', $api.namespace `
        -replace '\{CLIENT_CLASS\}', $api.clientClass `
        -replace '\{EXCEPTION_CLASS\}', $api.exceptionClass `
        -replace '\{OUTPUT\}', $api.outputFile
    
    # Save temporary config
    $tempConfig = Join-Path $PSScriptRoot "nswag-temp-$($api.name).json"
    $configContent | Out-File -FilePath $tempConfig -Encoding UTF8
    
    try {
        # Run NSwag
        nswag run $tempConfig 2>&1 | Out-Null
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  Status: SUCCESS" -ForegroundColor Green
            $successCount++
        } else {
            Write-Host "  Status: FAILED (exit code: $LASTEXITCODE)" -ForegroundColor Red
            $failCount++
        }
    }
    catch {
        Write-Host "  Status: ERROR - $($_.Exception.Message)" -ForegroundColor Red
        $failCount++
    }
    finally {
        # Clean up temp file
        if (Test-Path $tempConfig) {
            Remove-Item $tempConfig -Force
        }
    }
    
    Write-Host ""
}

# Summary
Write-Host "=== Generation Summary ===" -ForegroundColor Cyan
Write-Host "  Success: $successCount" -ForegroundColor Green
Write-Host "  Failed: $failCount" -ForegroundColor $(if ($failCount -gt 0) { "Red" } else { "Gray" })
Write-Host "  Total: $($config.apis.Count)" -ForegroundColor Gray
Write-Host ""

if ($failCount -eq 0) {
    Write-Host "All clients generated successfully!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "Some clients failed to generate" -ForegroundColor Yellow
    exit 1
}
