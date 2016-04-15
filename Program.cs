using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CleanUpProdTables
{
    class Program
    {
        static void Main(string[] args)
        {
            DateTime startTime = DateTime.UtcNow;
            int accountsNumber = Consts.Accounts.Count;

            Task<long[]> parent = new Task<long[]>(() => {
                var results = new long[accountsNumber];

                for (int i = 0; i < accountsNumber; i++)
                {
                    // This loop is running in parallel so if directly use i, i could be equal to accountsNumber
                    int index = i;
                    new Task(() => results[index] = CleanUpAccount(Consts.Accounts[index].Name, Consts.Accounts[index].Key),
                        TaskCreationOptions.AttachedToParent).Start();
                }

                return results;
            });

            // When the parent and its children have run to completion, display the results
            long sum = 0;
            var cwt = parent.ContinueWith(p => {
                DateTime endTime = DateTime.UtcNow;
                Console.WriteLine("Start Time: {0}", startTime.ToString(Consts.DateFormat));
                Console.WriteLine("End Time  : {0}", endTime.ToString(Consts.DateFormat));
                Console.WriteLine("Time Cost : {0}", endTime - startTime);

                Array.ForEach(p.Result, result => sum += result);
                Console.WriteLine("Deleted Entities: {0}", sum);
            });

            // Start the parent Task so it can start its children
            parent.Start();

            cwt.Wait();
            Console.WriteLine("All tasks done, press any key to exit.");
            Console.ReadKey();
        }

        static void ShowResult(long result)
        {
            Console.WriteLine("Child Task done. Totally {0} entities deleted.", result);
        }

        static CloudTableClient GetClient(string connectionString)
        {
            var account = CloudStorageAccount.Parse(connectionString);
            var client = account.CreateCloudTableClient();

            return client;
        }

        static CloudTable GetTable(CloudTableClient client, string tableName)
        {
            return client.GetTableReference(tableName);
        }

        static string GetStartDateString(DateTime date)
        {
            return date.ToString(Consts.DateFormat);
        }

        static TableQuery<DynamicTableEntity> BuildQuery(DateTime startDate, int takeCount)
        {
            var query = new TableQuery<DynamicTableEntity>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, GetStartDateString(startDate)))
                .Select(new string[] { "PartitionKey", "RowKey" })
                .Take(takeCount);

            return query;
        }

        static long DeleteEntitiesInTable(string accountName, CloudTable table)
        {
            Console.WriteLine("Start cleaning up Account:{0} Table:{1}", accountName, table.Name);

            var sum = 0;
            var startDate = DateTime.UtcNow.AddDays(-Consts.RetentionDays);
            var dateString = GetStartDateString(startDate);
            var query = BuildQuery(startDate, Consts.BatchSize);
            Console.WriteLine("QueryString: {0}", query.FilterString);

            while (true)
            {
                var entities = table.ExecuteQuery(query);
                var count = 0;
                foreach (var entry in entities)
                {
                    try
                    {
                        var result = table.Execute(TableOperation.Delete(entry));
                        if (result.HttpStatusCode != 204)
                        {
                            Console.WriteLine("Unexpected status code: {0}", result.HttpStatusCode);
                        }
                        count++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception: {0}", ex.Message);
                    }
                }

                sum += count;
                if (count <= 0) break;

                Console.WriteLine("Batch delete done in Account:{0} Table: {1}. {2} entries removed.", accountName, table.Name, count);
            }

            Console.WriteLine("Table cleanup done for Account:{0} Table: {1}.", accountName, table.Name);
            Console.WriteLine("  {0} rows in total.\n", sum);
            return sum;
        }
        static long BatchDeleteEntitiesInTable(string accountName, CloudTable table)
        {
            Console.WriteLine("Start cleaning up Account:{0} Table:{1}", accountName, table.Name);

            var sum = 0;
            var startDate = DateTime.UtcNow.AddDays(-Consts.RetentionDays);
            var dateString = GetStartDateString(startDate);
            var query = BuildQuery(startDate, Consts.BatchSize);
            Console.WriteLine("QueryString: {0}", query.FilterString);

            while (true)
            {
                var batchOperation = new TableBatchOperation();
                var entities = table.ExecuteQuery(query);
                var count = 0;
                var batchPartitionKey = String.Empty;
                foreach (var entry in entities)
                {
                    // a Batch Operation allows a maximum 100 entities in the batch which must share the same PartitionKey 
                    // so we split the batch if the partition key is different or count exceed maximum
                    if (batchPartitionKey == String.Empty)
                    {
                        batchPartitionKey = entry.PartitionKey;
                        batchOperation.Delete(entry);
                        count++;
                    }
                    else if ((batchPartitionKey != entry.PartitionKey) || (count >= Consts.BatchSize))
                    {
                        break;
                    }
                    else
                    {
                        batchOperation.Delete(entry);
                        count++;
                    }
                }

                sum += count;
                if (count <= 0) break;

                try
                {
                    var result = table.ExecuteBatch(batchOperation);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: {0}", ex.Message);
                }

                Console.WriteLine("Batch delete done in Account:{0} Table: {1}.", accountName, table.Name);
                Console.WriteLine("  Partition Key:'{0}'. {1} entries removed.", batchPartitionKey, count);
            }

            Console.WriteLine("Table cleanup done for Account:{0} Table: {1}.", accountName, table.Name);
            Console.WriteLine("  {0} rows in total.\n", sum);
            return sum;
        }

        static long CleanUpAccount(string accountName, string connectionString)
        {
            Console.WriteLine("Start cleaning up account: {0}", accountName);

            long sum = 0;
            var tasks = new List<Task<long>>();
            var client = GetClient(connectionString);

            for (int i = 0; i < Consts.TableNames.Length; i++)
            {
                var index = i;
                var table = GetTable(client, Consts.TableNames[index]);

                // Since table PartitionKey is in format like 'MM-dd-yyyy HH:mm:ss', and batch operation requires all operations have same PartitionKey
                // Only for these 2 tables there are usually multiple entities under same PartitionKey and Batch operation could be faster
                // For other tables, batch operation is even slower, since after each deletion we need to query again
                if (table.Name == "Performance" || table.Name == "Trace")
                {
                    tasks.Add(Task<long>.Factory.StartNew(() => BatchDeleteEntitiesInTable(accountName, table)));
                }
                else
                {
                    tasks.Add(Task<long>.Factory.StartNew(() => DeleteEntitiesInTable(accountName, table)));
                }
            }

            Task.WaitAll(tasks.ToArray());
            foreach (var task in tasks)
            {
                sum += task.Result;
            }

            Console.WriteLine("Cleaning up account: {0} done. {1} entities deleted in total", accountName, sum);
            return sum;
        }
    }
}
