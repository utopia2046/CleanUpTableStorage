# CleanUpTableStorage
work util

Purpose: To clean up old data on Azure table storage. 
Status:  In progress

First I use Powershell script and run them in an Automation Runbook. Azure powershell doesn't provide cmdlet at entity level so I try some PowerShell functions to create Microsoft.WindowsAzure.Storage.Table.TableQuery objects and delete the returned entities. This method is slow, I tried to use BatchOperation to accelerate it but BatchOperation have limits that in one batch, all entities must have same PrimaryKey, this doesn't work for me since the table I need to clean up is using time string as Primary Key "MM-dd-yyyy HH:mm:ss". In some tables, each entity has a different Primary Key, which causes the BatchOpertion to be even slow.

Then I try to parelle the loop by using WorkFlow Runbook. But when change the script type to WorkFlow with same logic, an exception thrown claiming that cannot to convert the Azure Storage Context to Deserialized Storage Context. After searching the document I found that to fix this issue I have to put the code creating and using type Context into InlineScript{} blocks, which means that they cannot use the Parelle keyword.

Finially I decide to write a console application using C# code to do this job. When it is done. I'll wrap the logic into a cmdlet module and upload the module to Azure.
