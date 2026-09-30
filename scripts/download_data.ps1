param([switch]$Download)
$ErrorActionPreference = 'Stop'
$endpoint = 'https://de-tech-assessment-396587179375-us-east-1-an.s3.us-east-1.amazonaws.com'
$projectRoot = Split-Path -Parent $PSScriptRoot
$objects = @()
$token = $null
do {
    $uri = "$endpoint/?list-type=2&prefix=data%2F"
    if ($token) { $uri += '&continuation-token=' + [uri]::EscapeDataString($token) }
    [xml]$listing = (Invoke-WebRequest -Uri $uri).Content
    $objects += @($listing.ListBucketResult.Contents | Where-Object { $_.Key -and -not $_.Key.EndsWith('/') })
    $token = $listing.ListBucketResult.NextContinuationToken
} while ($listing.ListBucketResult.IsTruncated -eq 'true')
$objects | Select-Object Key,Size
$totalBytes = ($objects | ForEach-Object { [long]$_.Size } | Measure-Object -Sum).Sum
Write-Host ("Objects: {0}; total: {1:N2} MiB" -f $objects.Count, ($totalBytes / 1MB))
if (-not $Download) { return }
foreach ($object in $objects) {
    $key = [string]$object.Key
    $destination = [IO.Path]::GetFullPath((Join-Path $projectRoot $key))
    $dataRoot = [IO.Path]::GetFullPath((Join-Path $projectRoot 'data')) + [IO.Path]::DirectorySeparatorChar
    if (-not $destination.StartsWith($dataRoot, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Unsafe object key: $key"
    }
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $destination) | Out-Null
    $escapedKey = (($key.Split('/') | ForEach-Object { [uri]::EscapeDataString($_) }) -join '/')
    $temporary = "$destination.part"
    Invoke-WebRequest -Uri "$endpoint/$escapedKey" -OutFile $temporary
    if ((Get-Item -LiteralPath $temporary).Length -ne [long]$object.Size) { throw "Size mismatch: $key" }
    Move-Item -LiteralPath $temporary -Destination $destination -Force
}
Write-Host 'Download complete. Raw files are excluded from Git.'
