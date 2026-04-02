variable "resource_group_name"     { type = string }
variable "location"                { type = string }
variable "environment"             { type = string }
variable "project"                 { type = string }
variable "alert_email"             { type = string }
variable "storage_account_id"      { type = string }
variable "databricks_workspace_id" { type = string }
variable "tags"                    { type = map(string) }
