param(
    [string]$ConnectUrl = "http://localhost:8083"
)

$ErrorActionPreference = "Stop"

$connectorFiles = @(
    "debezium-connector.json",
    "elasticsearch-sink.json"
)

foreach ($file in $connectorFiles) {
    $definition = Get-Content -Raw (Join-Path $PSScriptRoot $file) | ConvertFrom-Json
    $config = $definition.config | ConvertTo-Json -Depth 20
    $uri = "$($ConnectUrl.TrimEnd('/'))/connectors/$($definition.name)/config"

    Invoke-RestMethod `
        -Method Put `
        -Uri $uri `
        -ContentType "application/json" `
        -Body $config | Out-Null

    Write-Host "Registered $($definition.name) from $file"
}
