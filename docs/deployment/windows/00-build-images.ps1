[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [switch]$NoCache,
    [switch]$Push
)

$ErrorActionPreference = "Stop"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..\..")
Set-Location $RepoRoot

$images = @(
    @{ Dockerfile = "docker/Dockerfile.batch"; Tag = "mquan1503/bigdata-batch:latest" },
    @{ Dockerfile = "docker/Dockerfile.simulator"; Tag = "mquan1503/bigdata-simulator:latest" },
    @{ Dockerfile = "docker/Dockerfile.streaming"; Tag = "mquan1503/bigdata-streaming:latest" },
    @{ Dockerfile = "docker/Dockerfile.dashboard"; Tag = "mquan1503/bigdata-dashboard:latest" },
    @{ Dockerfile = "docker/Dockerfile.airflow"; Tag = "mquan1503/bigdata-airflow:latest" },
    @{ Dockerfile = "docker/Dockerfile.spark"; Tag = "mquan1503/bigdata-spark:latest" }
)

foreach ($image in $images) {
    $args = @("build", "-f", $image.Dockerfile, "-t", $image.Tag)
    if ($NoCache) { $args += "--no-cache" }
    $args += "."

    if ($PSCmdlet.ShouldProcess($image.Tag, "docker build")) {
        Write-Host "==> Building $($image.Tag)" -ForegroundColor Cyan
        & docker @args
    }

    if ($Push -and $PSCmdlet.ShouldProcess($image.Tag, "docker push")) {
        Write-Host "==> Pushing $($image.Tag)" -ForegroundColor Cyan
        & docker push $image.Tag
    }
}
