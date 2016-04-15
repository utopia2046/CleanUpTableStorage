
function LoadStorageDll
{
  $result = Get-ChildItem -Recurse -Filter "Microsoft.WindowsAzure.Storage.dll" -File
  foreach ($file in $result)
  {
    Write-Output $file.FullName
  }

  Add-Type -Path $result[0].FullName # "C:\Modules\Azure\Microsoft.WindowsAzure.Storage.dll"
  #Add-Type -Path "C:\Program Files (x86)\Microsoft SDKs\Azure\AzCopy\Microsoft.WindowsAzure.Storage.dll"
}

#---------------------------------------
# Functions
#---------------------------------------

# Return Production storage account and keys
function GetAccounts
{
  $dict = @{}
  
  $accountEU = Get-AutomationVariable -Name "AccountEU"
  $keyEU = Get-AutomationVariable -Name "KeyEU"
  $dict.Add($accountEU, $keyEU)
  
  $accountWU = Get-AutomationVariable -Name "AccountWU"
  $keyWU = Get-AutomationVariable -Name "KeyWU"
  $dict.Add($accountWU, $keyWU)
  
  $accountNE = Get-AutomationVariable -Name "AccountNE"
  $keyNE = Get-AutomationVariable -Name "KeyNE"
  $dict.Add($accountNE, $keyNE)
  
  $accountSEA = Get-AutomationVariable -Name "AccountSEA"
  $keySEA = Get-AutomationVariable -Name "KeySEA"
  $dict.Add($accountSEA, $keySEA)
  
  $accountNCU = Get-AutomationVariable -Name "AccountNCU"
  $keyNCU = Get-AutomationVariable -Name "KeyNCU"
  $dict.Add($accountNCU, $keyNCU)

  return $dict
}

# Return Table Storage Names
function GetTableNames
{
  $list = New-Object System.Collections.Generic.List[string]
  
  $list.Add("Error")
  $list.Add("UserError")
  $list.Add("ApplicationError")
  $list.Add("Performance")
  $list.Add("Latency")
  $list.Add("Trace")
  
  return $list
}

# Return Column Names
function GetColumns
{
  $list = New-Object System.Collections.Generic.List[string]
  
  $list.Add("PartitionKey")
  $list.Add("RowKey")
  $list.Add("Timestamp")
  $list.Add("Message")
  
  return $list
}

# Return Today - RentenionDays date string in PartitionKey format
function GetStartDateString
{
  param([int] $retentionDays)

  $startDate = [DateTime]::UtcNow.AddDays(-$retentionDays)

  return $startDate.ToString("MM-dd-yyyy HH:mm:ss")
}

# Build table query object
function BuildQuery
{
  param([string] $dateString, [int] $takeCount)

  $query = New-Object Microsoft.WindowsAzure.Storage.Table.TableQuery
  $query.FilterString = "PartitionKey lt '$dateString'"
  $query.TakeCount = $takeCount
  
  return $query
}

# Query and delete entities in one table using given query
function DeleteEntitiesInTable
{
  param(
    [Microsoft.WindowsAzure.Commands.Common.Storage.AzureStorageContext] $context,
    [string] $tableName,
    [string] $dateString, 
    [int] $takeCount
    #[Microsoft.WindowsAzure.Storage.Table.TableQuery] $query
  )

  $query = New-Object Microsoft.WindowsAzure.Storage.Table.TableQuery
  $query.FilterString = "PartitionKey lt '$dateString'"
  $query.TakeCount = $takeCount

  Write-Output "Start cleaning up table: $tableName"
  Write-Output $query.FilterString

  $table = Get-AzureStorageTable UserError -Context $context
  $entities = $table.CloudTable.ExecuteQuery($query)
  
  while ($entities.Properties.Count -gt 0)
  {
    foreach ($entity in $entities)
    {
      try
      {
        $result = $table.CloudTable.Execute([Microsoft.WindowsAzure.Storage.Table.TableOperation]::Delete($entity))
        if ($result.HttpStatusCode -ne 204)
        {
          Write-Output "Unexpected Status code:" $result.HttpStatusCode
        }
      }
      catch
      {
        Write-Output $_.Exception.Message
      }
    }
    Write-Output "Batch delete done"
    $entities = $table.CloudTable.ExecuteQuery($query)
  }

  Write-Output "Table clean up done."
}

# Clean up all tables in one storage account
function CleanUpTablesInAccount
{
  param
  (
    [string] $accountName, 
    [string] $key, 
    [string] $dateString,
    [int] $batchSize
  )

  Write-Output "Clean up account: $accountName"
  Write-Output "Delete entity before than: $dateString"
  Write-Output "Batch size: $batchSize"

  $context = New-AzureStorageContext -StorageAccountName $accountName -StorageAccountKey $key

  $query = New-Object Microsoft.WindowsAzure.Storage.Table.TableQuery
  $query.FilterString = "PartitionKey lt '$dateString'"
  $query.TakeCount = $batchSize

  $tableNames = GetTableNames
  foreach ($tableName in $tableNames)
  {
    Write-Output "Start cleaning up table: $tableName"
    Write-Output $query.FilterString
    
    $table = Get-AzureStorageTable $tableName -Context $context
    $entities = $table.CloudTable.ExecuteQuery($query)
  
    while ($entities.Properties.Count -gt 0)
    {
      foreach ($entity in $entities)
      {
        try
        {
          $result = $table.CloudTable.Execute([Microsoft.WindowsAzure.Storage.Table.TableOperation]::Delete($entity))
          if ($result.HttpStatusCode -ne 204)
          {
            Write-Output "Unexpected Status code:" $result.HttpStatusCode
          }
        }
        catch
        {
          Write-Output $_.Exception.Message
        }
      }
      Write-Output "Batch delete done"
      $entities = $table.CloudTable.ExecuteQuery($query)
    }
    
    Write-Output "Table clean up done."
  }

  Write-Output "Account clean up done."
  Write-Output
}

#---------------------------------------
# Main
#---------------------------------------

Write-Output "Reading global variables"
$retentionDays = Get-AutomationVariable -Name "TableRetentionDays"
$dateString = GetStartDateString $retentionDays
Write-Output "Deleting entities earlier than $dateString"
$batchSize = Get-AutomationVariable -Name "TableBatchSize"
#$query = BuildQuery $dateString $batchSize

$accounts = GetAccounts
foreach ($account in $accounts.Keys)
{ 
    $key = $accounts[$account]
    CleanUpTablesInAccount $account $key $dateString $batchSize
}
