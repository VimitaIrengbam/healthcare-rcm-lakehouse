// =============================================================================
// Healthcare RCM Lakehouse — declarative infrastructure (Bicep)
//
// Azure-native alternative to infra/provision.ps1. Declares the core resources:
// ADLS Gen2 (+ containers), Key Vault, Azure SQL serverless, Databricks, Data Factory.
//
// Deploy into a NEW resource group (globally-unique names => change `suffix`):
//   az group create -n rg-rcm-demo2 -l eastus
//   az deployment group create -g rg-rcm-demo2 -f infra/main.bicep \
//       -p sqlAdminPassword='<strong-pwd>' keyVaultAdminObjectId='<your-oid>' suffix='abc123'
//
// NOT covered here (kept imperative in provision.ps1 / done out-of-band):
//   resource-provider registration, Key Vault secret values, RBAC/role assignments,
//   budget alerts, and SQL firewall for your client IP.
// =============================================================================

@description('Region for storage, Key Vault, Databricks, ADF.')
param location string = resourceGroup().location

@description('Region for Azure SQL (eastus/eastus2 are often capacity-blocked on trial subs; centralus works).')
param sqlLocation string = 'centralus'

@description('Suffix to make globally-unique names unique. Change for a fresh deployment.')
param suffix string = '70648c'

@description('SQL administrator login.')
param sqlAdminUser string = 'rcmadmin'

@secure()
@description('SQL administrator password (not stored in the template).')
param sqlAdminPassword string

@description('AAD object id granted Key Vault secret access (your user, or leave empty to skip).')
param keyVaultAdminObjectId string = ''

@description('AAD tenant id for Key Vault.')
param tenantId string = subscription().tenantId

var storageName    = 'strcmdemo${suffix}'
var keyVaultName   = 'kv-rcm-demo-${suffix}'
var sqlServerName  = 'sqlrcm${suffix}'
var databricksName = 'dbw-rcm-demo'
var adfName        = 'adf-rcm-demo-${suffix}'
var containers     = [ 'landing', 'bronze', 'silver', 'gold', 'quarantine', 'checkpoints', 'metadata' ]

// --------------------------------------------------------------------------- ADLS Gen2
resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageName
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true            // hierarchical namespace = ADLS Gen2
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource blob 'Microsoft.Storage/storageAccounts/blobServices@2023-01-01' = {
  parent: storage
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-01-01' = [for c in containers: {
  parent: blob
  name: c
}]

// --------------------------------------------------------------------------- Key Vault (access-policy model)
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  properties: {
    sku: { family: 'A', name: 'standard' }
    tenantId: tenantId
    enableRbacAuthorization: false   // access-policy model (immediate data-plane access)
    accessPolicies: empty(keyVaultAdminObjectId) ? [] : [
      {
        tenantId: tenantId
        objectId: keyVaultAdminObjectId
        permissions: { secrets: [ 'get', 'list', 'set', 'delete' ] }
      }
    ]
  }
}

// --------------------------------------------------------------------------- Azure SQL (serverless, auto-pause)
resource sqlServer 'Microsoft.Sql/servers@2023-05-01-preview' = {
  name: sqlServerName
  location: sqlLocation
  properties: {
    administratorLogin: sqlAdminUser
    administratorLoginPassword: sqlAdminPassword
    minimalTlsVersion: '1.2'
  }
}

resource sqlFirewallAzure 'Microsoft.Sql/servers/firewallRules@2023-05-01-preview' = {
  parent: sqlServer
  name: 'AllowAzureServices'
  properties: { startIpAddress: '0.0.0.0', endIpAddress: '0.0.0.0' }
}

resource sqlDb 'Microsoft.Sql/servers/databases@2023-05-01-preview' = {
  parent: sqlServer
  name: 'emr_source'
  location: sqlLocation
  sku: { name: 'GP_S_Gen5', tier: 'GeneralPurpose', family: 'Gen5', capacity: 1 }
  properties: {
    autoPauseDelay: 60          // strict: 60 = Azure minimum
    minCapacity: json('0.5')
    zoneRedundant: false
  }
}

// --------------------------------------------------------------------------- Databricks workspace
resource databricks 'Microsoft.Databricks/workspaces@2023-02-01' = {
  name: databricksName
  location: location
  sku: { name: 'premium' }       // premium required for Unity Catalog + secret scopes
  properties: {
    managedResourceGroupId: subscriptionResourceId(
      'Microsoft.Resources/resourceGroups',
      'databricks-rg-${databricksName}-${uniqueString(databricksName, resourceGroup().id)}'
    )
  }
}

// --------------------------------------------------------------------------- Data Factory
resource adf 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: adfName
  location: location
  identity: { type: 'SystemAssigned' }
}

// --------------------------------------------------------------------------- Outputs
output storageAccount string = storage.name
output keyVaultName string = keyVault.name
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output databricksWorkspaceUrl string = databricks.properties.workspaceUrl
output dataFactoryPrincipalId string = adf.identity.principalId
