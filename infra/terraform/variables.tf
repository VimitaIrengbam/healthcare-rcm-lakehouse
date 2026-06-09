variable "resource_group_name" {
  type        = string
  default     = "rg-rcm-demo"
  description = "Resource group to create and deploy into."
}

variable "location" {
  type        = string
  default     = "eastus"
  description = "Region for storage, Key Vault, Databricks, ADF."
}

variable "sql_location" {
  type        = string
  default     = "centralus"
  description = "Region for Azure SQL (eastus/eastus2 are often capacity-blocked on trial subs)."
}

variable "suffix" {
  type        = string
  default     = "70648c"
  description = "Suffix for globally-unique resource names. Change for a fresh deployment."
}

variable "sql_admin_user" {
  type        = string
  default     = "rcmadmin"
  description = "SQL administrator login."
}

variable "sql_admin_password" {
  type        = string
  sensitive   = true
  description = "SQL administrator password (pass via -var or TF_VAR_sql_admin_password; never commit)."
}

variable "kv_admin_object_id" {
  type        = string
  default     = ""
  description = "AAD object id granted Key Vault secret access. Empty = current deploying principal."
}

variable "tenant_id" {
  type        = string
  default     = ""
  description = "AAD tenant id. Empty = current."
}
