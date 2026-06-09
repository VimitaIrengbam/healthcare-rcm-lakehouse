// Parameters for main.bicep. Pass the secret at deploy time, don't commit it:
//   az deployment group create -g <rg> -f infra/main.bicep -p infra/main.bicepparam \
//       sqlAdminPassword='<strong-pwd>'
using './main.bicep'

param location = 'eastus'
param sqlLocation = 'centralus'
param suffix = '70648c'
param sqlAdminUser = 'rcmadmin'
// sqlAdminPassword: pass on the command line (secure), do not hard-code here.
param sqlAdminPassword = ''
// Your AAD object id (az ad signed-in-user show --query id -o tsv) to grant KV secret access:
param keyVaultAdminObjectId = ''
