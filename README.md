# CleanUpTableStorage
work util

Purpose: To clean up old data on Azure table storage. 

Status:  In progress

V1: I wrote PowerShell scripts and run them in an Automation Runbook. Azure PowerShell doesn't provide cmdlet at entity level so I wrote a group of PowerShell functions to create Microsoft.WindowsAzure.Storage.Table.TableQuery object and delete the returned entities of the query. This method is slow. Thus I use BatchOperation to accelerate it. Unfortunately BatchOperation have limit that in one batch, all entities must have same PrimaryKey. This doesn't work for me since the table I need to clean up is using time string like  "MM-dd-yyyy HH:mm:ss" as Primary Key. In some tables, each entity has a different Primary Key, which causes the BatchOpertion to be even slower.

V2: I try to parallel the loop by using WorkFlow Runbook. When changing the script type to WorkFlow with same logic, an exception is thrown claiming that it cannot convert the Azure Storage Context object to Deserialized Storage Context object. After searching the documents, I found that to fix this issue I have to put the code using Context type into InlineScript{} blocks, which means that they cannot use the Parallel keyword.

V3: I decide to write a console application using C# code to do this job. It creates multiple tasks, one task for each storage account, each table and each given time slot. I have 5 storage accounts and each have 6 tables. Among them, one storage account have most traffics. And in all tables, 2 tables are very busy while other tables are usually not. So usually it could clean up 1 day’s data in less than one hour.

Future Plan: I'm considering to wrap the logic into a cmdlet module and upload the module to Azure.
