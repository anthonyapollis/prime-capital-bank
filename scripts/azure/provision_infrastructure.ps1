<#
.SYNOPSIS
    Provision all Azure infrastructure for Prime Capital Bank.
.DESCRIPTION
    Creates: Resource Group, ADLS Gen2, Databricks workspace (Premium), Key Vault,
    Azure SQL Database (Business Critical), Log Analytics, Private Endpoints, RBAC.
.PARAMETER SubscriptionId
    Azure subscription ID to deploy into.
.PARAMETER AdminEmail
    Email address for alerts and admin access.
.EXAMPLE
    .\provision_infrastructure.ps1 -SubscriptionId "00000000-0000-0000-0000-000000000000" -AdminEmail "ops@primecapital.co.za"
#>

[CmdletBinding(SupportsShouldProcess)]
param(
    [Parameter(Mandatory)]
    [string]$SubscriptionId,

    [Parameter(Mandatory)]
    [string]$AdminEmail,

    [string]$Location          = "southafricanorth",
    [string]$ResourceGroup     = "rg-prime-capital-prod",
    [string]$AdlsAccount       = "primecapitaladls",
    [string]$DatabricksName    = "dbw-prime-capital-prod",
    [string]$KeyVaultName      = "kv-prime-capital-prod",
    [string]$SqlServerName     = "sql-prime-capital-prod",
    [string]$SqlDatabaseName   = "sqldb-prime-capital-reporting",
    [string]$LogAnalyticsName  = "law-prime-capital-prod",
    [string]$VnetName          = "vnet-prime-capital-prod",
    [string]$SubnetName        = "snet-private-endpoints",
    [string]$Environment       = "prod",
    [switch]$SkipLogin
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ── Logging ────────────────────────────────────────────────────────────────────
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $colour = switch ($Level) {
        "INFO"    { "Cyan" }
        "SUCCESS" { "Green" }
        "WARN"    { "Yellow" }
        "ERROR"   { "Red" }
        default   { "White" }
    }
    Write-Host "[$ts] [$Level] $Message" -ForegroundColor $colour
}

function Invoke-AzCli {
    param([string]$Command, [string]$Description)
    Write-Log "az $($Command.Substring(0, [Math]::Min($Command.Length, 80)))..." "INFO"
    $result = Invoke-Expression "az $Command" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "Azure CLI command failed: az $Command`nError: $result"
    }
    return $result
}

# ── Variables ──────────────────────────────────────────────────────────────────
$Tags        = "environment=$Environment project=prime-capital-bank owner=data-engineering"
$TagsJson    = @{ environment = $Environment; project = "prime-capital-bank"; owner = "data-engineering" } | ConvertTo-Json -Compress

$ADLS_CONTAINERS = @("raw", "bronze", "silver", "gold", "archive", "ml-models")
$SqlAdminUser    = "pcb-sql-admin"

Write-Log "======================================================" "INFO"
Write-Log "  Prime Capital Bank — Azure Infrastructure Provisioning" "INFO"
Write-Log "  Subscription : $SubscriptionId" "INFO"
Write-Log "  Location     : $Location" "INFO"
Write-Log "  Resource Grp : $ResourceGroup" "INFO"
Write-Log "======================================================" "INFO"

# ── 1. Login & Subscription ───────────────────────────────────────────────────
if (-not $SkipLogin) {
    Write-Log "Logging in to Azure..."
    try { az login --output none } catch { Write-Log "az login failed — ensure Azure CLI is installed" "ERROR"; throw }
}

Invoke-AzCli "account set --subscription $SubscriptionId" "Set subscription"
Write-Log "Subscription set: $SubscriptionId" "SUCCESS"

# ── 2. Resource Group ─────────────────────────────────────────────────────────
Write-Log "Creating resource group $ResourceGroup..."
try {
    Invoke-AzCli "group create --name $ResourceGroup --location $Location --tags $Tags" "Create RG"
    Write-Log "Resource group created: $ResourceGroup" "SUCCESS"
} catch {
    Write-Log "Resource group may already exist: $_" "WARN"
}

# ── 3. Virtual Network & Subnet (for Private Endpoints) ──────────────────────
Write-Log "Creating VNet for private endpoints..."
try {
    Invoke-AzCli "network vnet create --resource-group $ResourceGroup --name $VnetName --location $Location --address-prefix 10.0.0.0/16 --subnet-name $SubnetName --subnet-prefix 10.0.1.0/24 --tags $Tags" "Create VNet"

    # Disable private endpoint network policies on subnet
    Invoke-AzCli "network vnet subnet update --resource-group $ResourceGroup --vnet-name $VnetName --name $SubnetName --disable-private-endpoint-network-policies true" "Disable PE policies"

    Write-Log "VNet created: $VnetName" "SUCCESS"
} catch {
    Write-Log "VNet creation issue: $_" "WARN"
}

# ── 4. ADLS Gen2 Storage Account ─────────────────────────────────────────────
Write-Log "Creating ADLS Gen2 account: $AdlsAccount..."
try {
    Invoke-AzCli "storage account create ``
        --name $AdlsAccount ``
        --resource-group $ResourceGroup ``
        --location $Location ``
        --sku Standard_LRS ``
        --kind StorageV2 ``
        --enable-hierarchical-namespace true ``
        --min-tls-version TLS1_2 ``
        --allow-blob-public-access false ``
        --https-only true ``
        --tags $Tags" "Create ADLS Gen2"

    Write-Log "ADLS Gen2 account created: $AdlsAccount" "SUCCESS"

    # Retrieve storage key for Key Vault secret
    $StorageKey = (Invoke-AzCli "storage account keys list --account-name $AdlsAccount --resource-group $ResourceGroup --query '[0].value' --output tsv" "Get storage key").Trim()

    # Create containers
    foreach ($container in $ADLS_CONTAINERS) {
        Write-Log "Creating container: $container"
        try {
            Invoke-AzCli "storage container create --name $container --account-name $AdlsAccount --auth-mode login" "Create container $container"
        } catch {
            Write-Log "Container $container may already exist: $_" "WARN"
        }
    }
    Write-Log "All ADLS containers created" "SUCCESS"
} catch {
    Write-Log "ADLS creation failed: $_" "ERROR"
    throw
}

# ── 5. Key Vault ──────────────────────────────────────────────────────────────
Write-Log "Creating Key Vault: $KeyVaultName..."
try {
    Invoke-AzCli "keyvault create ``
        --name $KeyVaultName ``
        --resource-group $ResourceGroup ``
        --location $Location ``
        --sku standard ``
        --enable-soft-delete true ``
        --retention-days 90 ``
        --enable-purge-protection true ``
        --tags $Tags" "Create Key Vault"

    Write-Log "Key Vault created: $KeyVaultName" "SUCCESS"

    # Store storage key
    Invoke-AzCli "keyvault secret set --vault-name $KeyVaultName --name 'storage-key' --value '$StorageKey'" "Set storage-key secret"

    # Generate SQL admin password
    $SqlPassword = -join ((65..90) + (97..122) + (48..57) + (33,35,36,64) |
                    Get-Random -Count 24 | ForEach-Object { [char]$_ })
    Invoke-AzCli "keyvault secret set --vault-name $KeyVaultName --name 'sql-admin-password' --value '$SqlPassword'" "Set sql-admin-password secret"

    # Placeholder secrets (updated after Databricks workspace creation)
    Invoke-AzCli "keyvault secret set --vault-name $KeyVaultName --name 'databricks-token' --value 'PLACEHOLDER_UPDATE_AFTER_DATABRICKS_CREATION'" "Set databricks-token placeholder"
    Invoke-AzCli "keyvault secret set --vault-name $KeyVaultName --name 'sendgrid-api-key' --value 'PLACEHOLDER_UPDATE_MANUALLY'" "Set sendgrid-api-key placeholder"

    Write-Log "Key Vault secrets configured" "SUCCESS"
} catch {
    Write-Log "Key Vault creation failed: $_" "ERROR"
    throw
}

# ── 6. Databricks Workspace (Premium) ─────────────────────────────────────────
Write-Log "Creating Databricks workspace: $DatabricksName (Premium)..."
try {
    Invoke-AzCli "databricks workspace create ``
        --name $DatabricksName ``
        --resource-group $ResourceGroup ``
        --location $Location ``
        --sku premium ``
        --tags $Tags" "Create Databricks workspace"

    Write-Log "Databricks workspace created: $DatabricksName" "SUCCESS"

    # Retrieve workspace URL
    $DatabricksUrl = (Invoke-AzCli "databricks workspace show --name $DatabricksName --resource-group $ResourceGroup --query 'workspaceUrl' --output tsv" "Get Databricks URL").Trim()
    Write-Log "Databricks URL: https://$DatabricksUrl" "INFO"

    # Store workspace URL in Key Vault
    Invoke-AzCli "keyvault secret set --vault-name $KeyVaultName --name 'databricks-workspace-url' --value 'https://$DatabricksUrl'" "Store Databricks URL"
} catch {
    Write-Log "Databricks creation failed: $_" "ERROR"
    throw
}

# ── 7. Azure SQL Database (Business Critical, 8 vCores) ──────────────────────
Write-Log "Creating Azure SQL Server: $SqlServerName..."
try {
    Invoke-AzCli "sql server create ``
        --name $SqlServerName ``
        --resource-group $ResourceGroup ``
        --location $Location ``
        --admin-user $SqlAdminUser ``
        --admin-password '$SqlPassword'" "Create SQL Server"

    Write-Log "Creating SQL Database (Business Critical, 8 vCores)..."
    Invoke-AzCli "sql db create ``
        --name $SqlDatabaseName ``
        --server $SqlServerName ``
        --resource-group $ResourceGroup ``
        --service-objective BC_Gen5_8 ``
        --backup-storage-redundancy Geo ``
        --zone-redundant true ``
        --tags $Tags" "Create SQL Database"

    Write-Log "Azure SQL Database created: $SqlDatabaseName" "SUCCESS"

    # Store connection string
    $SqlConnStr = "Server=tcp:$SqlServerName.database.windows.net,1433;Database=$SqlDatabaseName;User=$SqlAdminUser;Password=$SqlPassword;Encrypt=True;TrustServerCertificate=False;"
    Invoke-AzCli "keyvault secret set --vault-name $KeyVaultName --name 'sql-connection-string' --value '$SqlConnStr'" "Store SQL connection string"
} catch {
    Write-Log "SQL Database creation failed: $_" "ERROR"
    throw
}

# ── 8. Log Analytics Workspace ────────────────────────────────────────────────
Write-Log "Creating Log Analytics workspace: $LogAnalyticsName..."
try {
    Invoke-AzCli "monitor log-analytics workspace create ``
        --workspace-name $LogAnalyticsName ``
        --resource-group $ResourceGroup ``
        --location $Location ``
        --retention-time 90 ``
        --tags $Tags" "Create Log Analytics"

    $WorkspaceId = (Invoke-AzCli "monitor log-analytics workspace show --workspace-name $LogAnalyticsName --resource-group $ResourceGroup --query 'customerId' --output tsv" "Get workspace ID").Trim()
    Write-Log "Log Analytics workspace created: $LogAnalyticsName (ID: $WorkspaceId)" "SUCCESS"
} catch {
    Write-Log "Log Analytics creation failed: $_" "ERROR"
    throw
}

# ── 9. Private Endpoints ──────────────────────────────────────────────────────
Write-Log "Creating Private Endpoints..."

function New-PrivateEndpoint {
    param([string]$Name, [string]$ResourceId, [string]$GroupId)
    try {
        Invoke-AzCli "network private-endpoint create ``
            --name $Name ``
            --resource-group $ResourceGroup ``
            --vnet-name $VnetName ``
            --subnet $SubnetName ``
            --private-connection-resource-id '$ResourceId' ``
            --group-id $GroupId ``
            --connection-name '$Name-connection' ``
            --location $Location" "Create PE: $Name"
        Write-Log "Private endpoint created: $Name" "SUCCESS"
    } catch {
        Write-Log "PE creation failed for $Name (may not be supported in region): $_" "WARN"
    }
}

# ADLS private endpoint
$AdlsId = (Invoke-AzCli "storage account show --name $AdlsAccount --resource-group $ResourceGroup --query 'id' --output tsv" "Get ADLS ID").Trim()
New-PrivateEndpoint -Name "pe-adls-dfs" -ResourceId $AdlsId -GroupId "dfs"

# Key Vault private endpoint
$KvId = (Invoke-AzCli "keyvault show --name $KeyVaultName --resource-group $ResourceGroup --query 'id' --output tsv" "Get KV ID").Trim()
New-PrivateEndpoint -Name "pe-keyvault" -ResourceId $KvId -GroupId "vault"

# SQL private endpoint
$SqlId = (Invoke-AzCli "sql server show --name $SqlServerName --resource-group $ResourceGroup --query 'id' --output tsv" "Get SQL ID").Trim()
New-PrivateEndpoint -Name "pe-sql" -ResourceId $SqlId -GroupId "sqlServer"

# ── 10. RBAC Roles ─────────────────────────────────────────────────────────────
Write-Log "Configuring RBAC roles..."
try {
    # Get current user's object ID
    $CurrentUserOid = (Invoke-AzCli "ad signed-in-user show --query 'objectId' --output tsv" "Get current user OID").Trim()

    # Storage: admin gets Blob Data Owner
    Invoke-AzCli "role assignment create --assignee $CurrentUserOid --role 'Storage Blob Data Owner' --scope $AdlsId" "RBAC: Storage Blob Data Owner"

    # Key Vault: admin gets Key Vault Administrator
    Invoke-AzCli "role assignment create --assignee $CurrentUserOid --role 'Key Vault Administrator' --scope $KvId" "RBAC: Key Vault Administrator"

    Write-Log "RBAC roles assigned" "SUCCESS"
} catch {
    Write-Log "RBAC assignment issue: $_" "WARN"
}

# ── 11. Enable Diagnostic Settings ───────────────────────────────────────────
Write-Log "Enabling diagnostic settings..."
try {
    $LawId = (Invoke-AzCli "monitor log-analytics workspace show --workspace-name $LogAnalyticsName --resource-group $ResourceGroup --query 'id' --output tsv" "Get LAW ID").Trim()

    Invoke-AzCli "monitor diagnostic-settings create --name 'adls-diagnostics' --resource $AdlsId --workspace $LawId --logs '[{\"category\":\"StorageRead\",\"enabled\":true},{\"category\":\"StorageWrite\",\"enabled\":true},{\"category\":\"StorageDelete\",\"enabled\":true}]' --metrics '[{\"category\":\"Transaction\",\"enabled\":true}]'" "ADLS diagnostics"

    Write-Log "Diagnostic settings enabled" "SUCCESS"
} catch {
    Write-Log "Diagnostic settings issue (non-fatal): $_" "WARN"
}

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Log ""
Write-Log "=====================================================" "SUCCESS"
Write-Log "  PROVISIONING COMPLETE" "SUCCESS"
Write-Log "=====================================================" "SUCCESS"
Write-Log "  Resource Group  : $ResourceGroup" "SUCCESS"
Write-Log "  ADLS Gen2       : $AdlsAccount.dfs.core.windows.net" "SUCCESS"
Write-Log "  Databricks      : https://$DatabricksUrl" "SUCCESS"
Write-Log "  Key Vault       : $KeyVaultName.vault.azure.net" "SUCCESS"
Write-Log "  SQL Server      : $SqlServerName.database.windows.net" "SUCCESS"
Write-Log "  Log Analytics   : $LogAnalyticsName" "SUCCESS"
Write-Log "" "SUCCESS"
Write-Log "  NEXT STEPS:" "INFO"
Write-Log "  1. Generate Databricks PAT and update kv secret 'databricks-token'" "INFO"
Write-Log "  2. Run deploy_databricks_resources.py" "INFO"
Write-Log "  3. Update SendGrid API key in Key Vault" "INFO"
Write-Log "=====================================================" "SUCCESS"
