#---------------------------------------
# Functions
#---------------------------------------

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

function DeleteEntitiesInTable
{
  param([string] $tableName, [Microsoft.WindowsAzure.Storage.Table.TableQuery] $query)

  Write-Output "Start cleaning up table: $tableName"
  Write-Output $query.FilterString

  $table = Get-AzureStorageTable UserError -Context $Context
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

#---------------------------------------
# Main
#---------------------------------------

Write-Output "Reading global variables"
$accountName = Get-AutomationVariable -Name "SiAccountName"
$key = Get-AutomationVariable -Name "SiAccountKey"
$retentionDays = Get-AutomationVariable -Name "TableRetentionDays"
$batchSize = Get-AutomationVariable -Name "TableBatchSize"
Write-Output "AccountName: $accountName BatchSize: $batchSize"

$context = New-AzureStorageContext -StorageAccountName $accountName -StorageAccountKey $key
$dateString = GetStartDateString $retentionDays
Write-Output "Deleting entities earlier than $dateString"

$tableNames = GetTableNames
$query = BuildQuery $dateString $batchSize

foreach ($tableName in $tableNames)
{
  DeleteEntitiesInTable $tableName $query
}
