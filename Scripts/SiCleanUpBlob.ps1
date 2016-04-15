# Reading shared variables
$accountName = Get-AutomationVariable -Name "SiAccountName"
$key = Get-AutomationVariable -Name "SiAccountKey"
$containerName = Get-AutomationVariable -Name "BlobContainerName"
$retentionDays = Get-AutomationVariable -Name "BlobRetentionDays"
$batchSize = Get-AutomationVariable -Name "BlobBatchSize"
Write-Output "AccountName: $accountName Container: $containerName RetentionDays: $retentionDays BatchSize: $batchSize"

$startDate = [DateTime]::UtcNow.AddDays(-$retentionDays);
Write-Output "Deleting files earlier than $startDate";

# Delete blob files that are older than startDate, if any exception, the file will be ignored and deleted next time, so there is no retry
$context = New-AzureStorageContext -StorageAccountName $accountName -StorageAccountKey $key;
Get-AzureStorageBlob -Context $context -Container $containerName -MaxCount $batchSize |
    Where-Object { $_.LastModified.UtcDateTime -lt $startDate } |
    Remove-AzureStorageBlob

Write-Output "Job done"
