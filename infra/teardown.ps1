<#
.SYNOPSIS
  Deletes the entire RCM demo resource group to stop all spend (Phase 0 / cost hygiene).

.DESCRIPTION
  One-liner teardown: removes the resource group named in config/env.json and everything in it.
  Run this after every work session on the Azure trial.

.NOTES
  Run from repo root:  ./infra/teardown.ps1
#>

[CmdletBinding()]
param(
  [string]$ConfigPath = "$PSScriptRoot/../config/env.json",
  [switch]$Force
)

$ErrorActionPreference = "Stop"
$cfg = Get-Content $ConfigPath -Raw | ConvertFrom-Json
$rg = $cfg.resourceGroup

if (-not $Force) {
  Write-Host "This will DELETE resource group '$rg' and ALL resources in it." -ForegroundColor Yellow
  $confirm = Read-Host "Type the resource group name to confirm"
  if ($confirm -ne $rg) {
    Write-Host "Aborted (confirmation did not match)." -ForegroundColor Red
    exit 1
  }
}

Write-Host "==> Deleting resource group: $rg"
az group delete --name $rg --yes --no-wait
Write-Host "Delete initiated (running in background). Verify in the portal that '$rg' is gone." -ForegroundColor Green
