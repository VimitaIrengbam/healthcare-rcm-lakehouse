<#
.SYNOPSIS
  Provisions the Azure footprint for the Healthcare RCM Lakehouse (Phase 0).

.DESCRIPTION
  Reads config/env.json and creates: resource group, ADLS Gen2 (with containers),
  Azure SQL serverless DB, Databricks workspace, Data Factory, Key Vault, and a budget alert.
  Cheapest viable SKUs are used. Re-running is mostly idempotent (az create calls are safe to repeat).

.NOTES
  Prereqs: Azure CLI logged in (`az login`), and the Databricks CLI for later steps.
  Run from repo root:  ./infra/provision.ps1
#>

[CmdletBinding()]
param(
  [string]$ConfigPath = "$PSScriptRoot/../config/env.json"
)

# NOTE: do NOT use "Stop" here. In Windows PowerShell 5.1, an `az` command writing a benign
# WARNING to stderr (e.g. extension auto-install notices) is promoted to a terminating error
# under Stop, which aborts provisioning mid-way. We use Continue and rely on the existence
# guards + the final verification block to catch real failures.
$ErrorActionPreference = "Continue"
$cfg = Get-Content $ConfigPath -Raw | ConvertFrom-Json

# Pre-install CLI extensions so their first-use stderr notices don't interrupt resource creation.
az extension add --name databricks --only-show-errors 2>$null
az extension add --name datafactory --only-show-errors 2>$null

$rg       = $cfg.resourceGroup
$loc      = $cfg.location
$storage  = $cfg.storage.accountName
$sqlSrv   = $cfg.azureSql.serverName
$sqlDb    = $cfg.azureSql.databaseName
$sqlUser  = $cfg.azureSql.adminUser
# SQL can have its own region: some regions block new SQL servers on trial subs
# (RegionDoesNotAllowProvisioning). Falls back to the global location.
$sqlLoc   = if ($cfg.azureSql.location) { $cfg.azureSql.location } else { $loc }
$kv       = $cfg.keyVault.name
$dbw      = $cfg.databricks.workspaceName
$adf      = $cfg.dataFactory.name

# ---------------------------------------------------------------------------
# Register resource providers (one-time per subscription; free).
# A brand-new subscription has these unregistered, which breaks resource creation.
# ---------------------------------------------------------------------------
$providers = @(
  "Microsoft.Storage", "Microsoft.Sql", "Microsoft.Databricks",
  "Microsoft.DataFactory", "Microsoft.KeyVault", "Microsoft.Network", "Microsoft.ManagedIdentity"
)
Write-Host "==> Registering resource providers (if needed)"
foreach ($rp in $providers) {
  $state = az provider show --namespace $rp --query registrationState -o tsv 2>$null
  if ($state -ne "Registered") {
    az provider register --namespace $rp --output none
    Write-Host "    registering $rp ..."
  }
}
# Wait until all are Registered (registration is async, usually < 5 min)
$deadline = (Get-Date).AddMinutes(10)
do {
  Start-Sleep -Seconds 15
  $pending = @()
  foreach ($rp in $providers) {
    if ((az provider show --namespace $rp --query registrationState -o tsv 2>$null) -ne "Registered") { $pending += $rp }
  }
  if ($pending.Count) { Write-Host "    waiting on: $($pending -join ', ')" }
} while ($pending.Count -and (Get-Date) -lt $deadline)
if ($pending.Count) { Write-Warning "Providers still pending after 10 min: $($pending -join ', '). Continuing anyway." }

Write-Host "==> Resource group: $rg ($loc)"
az group create --name $rg --location $loc --output none

# ---------------------------------------------------------------------------
# Key Vault + SQL admin password (generated, stored as a secret)
# ---------------------------------------------------------------------------
Write-Host "==> Key Vault: $kv"
# Use the access-policy model (not RBAC) so the creator gets data-plane access immediately
# (RBAC role propagation is slow and not granted automatically even to owners).
# keyvault create is NOT idempotent (errors if the vault exists), so guard it.
$kvNames = az keyvault list --resource-group $rg --query "[].name" -o tsv
if ($kvNames -notcontains $kv) {
  az keyvault create --name $kv --resource-group $rg --location $loc `
    --enable-rbac-authorization false --output none
} else {
  Write-Host "    vault exists; ensuring access-policy model"
  az keyvault update --name $kv --resource-group $rg --enable-rbac-authorization false --output none
}
$callerOid = az ad signed-in-user show --query id -o tsv
az keyvault set-policy --name $kv --object-id $callerOid `
  --secret-permissions get list set delete backup restore --output none

$sqlPwdSecret = $cfg.azureSql.adminPasswordSecretName
$existing = az keyvault secret list --vault-name $kv --query "[?name=='$sqlPwdSecret'] | length(@)" -o tsv
if ([int]$existing -eq 0) {
  $pwd = -join ((48..57) + (65..90) + (97..122) + (33,35,37,38) | Get-Random -Count 24 | ForEach-Object { [char]$_ })
  az keyvault secret set --vault-name $kv --name $sqlPwdSecret --value $pwd --output none
  Write-Host "    generated SQL admin password -> Key Vault secret '$sqlPwdSecret'"
}
$sqlPwd = az keyvault secret show --vault-name $kv --name $sqlPwdSecret --query value -o tsv

# ---------------------------------------------------------------------------
# ADLS Gen2 (hierarchical namespace) + containers
# ---------------------------------------------------------------------------
Write-Host "==> ADLS Gen2: $storage"
# Guard: re-creating an existing storage account emits a stderr WARNING which, under
# ErrorActionPreference=Stop in PS 5.1, becomes a terminating error. Skip if it exists.
$stExists = az storage account list --resource-group $rg --query "[?name=='$storage'] | [0].name" -o tsv 2>$null
if (-not $stExists) {
  az storage account create `
    --name $storage --resource-group $rg --location $loc `
    --sku Standard_LRS --kind StorageV2 --hns true --output none
} else {
  Write-Host "    storage account exists; skipping create"
}

$key = az storage account keys list --account-name $storage --resource-group $rg --query "[0].value" -o tsv
foreach ($c in $cfg.storage.containers) {
  az storage container create --name $c --account-name $storage --account-key $key --output none
  Write-Host "    container: $c"
}

# ---------------------------------------------------------------------------
# Azure SQL (serverless, auto-pause) as EMR source system
# ---------------------------------------------------------------------------
Write-Host "==> Azure SQL server: $sqlSrv ($sqlLoc)"
# Guard: SQL logical server names are reserved globally even after a failed/region-blocked
# create, and the create is not idempotent. Skip if it already exists in this RG.
$sqlExists = az sql server list --resource-group $rg --query "[?name=='$sqlSrv'] | [0].name" -o tsv 2>$null
if (-not $sqlExists) {
  az sql server create `
    --name $sqlSrv --resource-group $rg --location $sqlLoc `
    --admin-user $sqlUser --admin-password $sqlPwd --output none
} else {
  Write-Host "    SQL server exists; skipping create"
}

# Allow Azure services + your current IP (demo convenience)
az sql server firewall-rule create --resource-group $rg --server $sqlSrv `
  --name AllowAzure --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 --output none
$myIp = (Invoke-RestMethod -Uri "https://api.ipify.org")
az sql server firewall-rule create --resource-group $rg --server $sqlSrv `
  --name AllowMyIP --start-ip-address $myIp --end-ip-address $myIp --output none

Write-Host "==> Azure SQL DB (serverless): $sqlDb"
az sql db create `
  --name $sqlDb --resource-group $rg --server $sqlSrv `
  --edition GeneralPurpose --compute-model Serverless `
  --family Gen5 --capacity 1 `
  --auto-pause-delay $cfg.azureSql.autoPauseDelayMinutes `
  --min-capacity $cfg.azureSql.minCapacity --output none

# ---------------------------------------------------------------------------
# Databricks workspace
# ---------------------------------------------------------------------------
Write-Host "==> Databricks workspace: $dbw"
# Guard: the workspace generates a random managed resource group name; re-creating an existing
# workspace fails with ApplianceManagedResourceGroupMismatch. Skip if it already exists.
$dbwExists = az databricks workspace list --resource-group $rg --query "[?name=='$dbw'] | [0].name" -o tsv 2>$null
if (-not $dbwExists) {
  az databricks workspace create `
    --name $dbw --resource-group $rg --location $loc `
    --sku $cfg.databricks.sku --output none
} else {
  Write-Host "    Databricks workspace exists; skipping create"
}

# ---------------------------------------------------------------------------
# Data Factory
# ---------------------------------------------------------------------------
Write-Host "==> Data Factory: $adf"
az extension add --name datafactory --only-show-errors 2>$null
az datafactory create --resource-group $rg --factory-name $adf --location $loc --output none

# ---------------------------------------------------------------------------
# Budget alert (backstop against runaway spend)
# ---------------------------------------------------------------------------
Write-Host "==> Budget: $($cfg.budget.name) ($($cfg.budget.amount) USD)"
$start = (Get-Date -Day 1).ToString("yyyy-MM-dd")
$end   = (Get-Date -Day 1).AddYears(1).ToString("yyyy-MM-dd")
# Note: the `consumption budget` CLI is preview and often rejects RG-scoped budgets on trial
# subscriptions (HTTP 400). Check the exit code explicitly (try/catch won't catch native az
# failures under ErrorAction=Continue) and fall back to a portal reminder.
az consumption budget create `
  --budget-name $cfg.budget.name `
  --amount $cfg.budget.amount `
  --category Cost --time-grain Monthly `
  --start-date $start --end-date $end `
  --resource-group $rg --output none 2>$null
if ($LASTEXITCODE -eq 0) {
  Write-Host "    budget created (set alert thresholds in the portal if needed)"
} else {
  Write-Warning "Budget not created via CLI (preview API limitation on trial subs). Set a `$$($cfg.budget.amount) budget + alerts manually: Portal > Cost Management > Budgets."
}

# ---------------------------------------------------------------------------
# Verify the expected resources actually exist (since we run with ErrorAction=Continue)
# ---------------------------------------------------------------------------
Write-Host ""
Write-Host "==> Verifying resources in $rg"
$found = az resource list --resource-group $rg --query "[].{name:name, type:type}" -o json | ConvertFrom-Json
$expect = @(
  @{ n = $kv;      t = "Microsoft.KeyVault/vaults" },
  @{ n = $storage; t = "Microsoft.Storage/storageAccounts" },
  @{ n = $sqlSrv;  t = "Microsoft.Sql/servers" },
  @{ n = $dbw;     t = "Microsoft.Databricks/workspaces" },
  @{ n = $adf;     t = "Microsoft.DataFactory/factories" }
)
$missing = @()
foreach ($e in $expect) {
  $hit = $found | Where-Object { $_.name -eq $e.n -and $_.type -eq $e.t }
  if ($hit) { Write-Host "    OK    $($e.n)  [$($e.t)]" }
  else { Write-Host "    MISSING $($e.n)  [$($e.t)]" -ForegroundColor Red; $missing += $e.n }
}
if ($missing.Count) {
  Write-Warning "Missing resources: $($missing -join ', '). Re-run this script or check the portal."
}

Write-Host ""
Write-Host "Provisioning complete." -ForegroundColor Green
Write-Host "Next:"
Write-Host "  1) In Databricks, enable Unity Catalog and create catalog '$($cfg.databricks.catalog)' with schemas: $($cfg.databricks.schemas -join ', ')"
Write-Host "  2) Create a secret scope '$($cfg.keyVault.secretScope)' backed by Key Vault '$kv'."
Write-Host "  3) Run sql/01_create_emr_tables.sql against $sqlSrv/$sqlDb."
Write-Host "  4) REMEMBER to run ./infra/teardown.ps1 when done for the day."
