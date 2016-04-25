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
            if (args.Length != 2)
            {
                PrintHelp();
                return;
            }

            try
            {
                DateTime startTime = DateTime.Parse(args[0]);
                DateTime endTime = DateTime.Parse(args[1]);
                Console.WriteLine("Start time: {0}", startTime);
                Console.WriteLine("end time: {0}", endTime);

                CleanUp(startTime, endTime);
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return;
            }

            Console.WriteLine("All tasks done.");
        }

        static void PrintHelp()
        {
            Console.WriteLine("CleanUpProdTable <startDate> <endDate>");
            Console.WriteLine("Example:");
            Console.WriteLine("CleanUpProdTable 2016-1-8 2016-1-18");
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

        static string GetDateString(DateTime date)
        {
            return date.ToString(Consts.DateFormat);
        }

        static TableQuery<DynamicTableEntity> BuildQuery(string startTimeString, string endTimeString)
        {
            var queryString = String.Format("PartitionKey ge '{0}' and PartitionKey le '{1}'", startTimeString, endTimeString);

            var query = new TableQuery<DynamicTableEntity>()
                .Where(queryString)
                .Select(new string[] { "PartitionKey", "RowKey" })
                .Take(Consts.BatchSize);

            return query;
        }

        static string GenerateCompoundKey(string accountName, string tableName)
        {
            return String.Concat(accountName, "_", tableName);
        }

        static Dictionary<string, CloudTable> GenerateTableDict()
        {
            var tablesDict = new Dictionary<string, CloudTable>();

            foreach (var account in Consts.Accounts)
            {
                var client = GetClient(account.Key);
                foreach (var tableName in Consts.TableNames)
                {
                    var table = GetTable(client, tableName);
                    tablesDict.Add(GenerateCompoundKey(account.Name, tableName), table);
                }
            }

            return tablesDict;
        }

        static List<Tuple<string, string>> GenerateTimeSlotList(DateTime startTime, DateTime endTime, TimeSpan slotSize)
        {
            var slots = new List<Tuple<string, string>>();
            var time = startTime;

            while (time < endTime - slotSize)
            {
                var startTimeString = GetDateString(time);
                var endTimeString = GetDateString(time.Add(slotSize));
                slots.Add(new Tuple<string, string>(startTimeString, endTimeString));
                time += slotSize;
            }

            slots.Add(new Tuple<string, string>(GetDateString(time), GetDateString(endTime)));
            return slots;
        }

        static void DeleteEntitiesInTable(string key, CloudTable table, TableQuery<DynamicTableEntity> query)
        {
            var sum = 0;
            while (true)
            {
                var count = 0;
                var entities = table.ExecuteQuery(query);
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
                    catch (StorageException ex)
                    {
                        if (ex.RequestInformation.HttpStatusCode != 404)
                        {
                            Console.WriteLine("Exception: {0}", ex.Message);
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception: {0}", ex.Message);
                    }
                }

                Console.WriteLine("Batch done for {0}. {1} entities deleted.", key, count);
                Console.WriteLine("  Query: {0}.", query.FilterString);

                sum += count;
                if (count <= 0) break;
            }

            Console.WriteLine("Table cleanup done for {0}. {1} entries removed.", key, sum);
        }

        static void BatchDeleteEntitiesInTable(string key, CloudTable table, TableQuery<DynamicTableEntity> query)
        {
            var sum = 0;

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
                    Console.WriteLine("Batch done for {0}. {1} entities deleted.", key, count);
                    Console.WriteLine("  Query: {0}.", query.FilterString);
                }
                catch (StorageException ex)
                {
                    if (ex.RequestInformation.HttpStatusCode != 404)
                    {
                        Console.WriteLine("Exception: {0}", ex.Message);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: {0}", ex.Message);
                }
            }

            Console.WriteLine("Table cleanup done for {0}. {1} entries removed.", key, sum);
        }

        static void CleanUp(DateTime startTime, DateTime endTime)
        {
            var tasks = new List<Task>();
            var tables = GenerateTableDict();
            var slotSize = CleanUpProdTables.Properties.Settings.Default.TimeSlotSize;
            var timeSlots = GenerateTimeSlotList(startTime, endTime, slotSize);

            foreach (var slot in timeSlots)
            {
                var query = BuildQuery(slot.Item1, slot.Item2);
                foreach (var table in tables)
                {
                    if (table.Value.Name == "Performance" || table.Value.Name == "Trace")
                    {
                        tasks.Add(Task.Factory.StartNew(() => BatchDeleteEntitiesInTable(table.Key, table.Value, query)));
                    }
                    else
                    {
                        tasks.Add(Task.Factory.StartNew(() => DeleteEntitiesInTable(table.Key, table.Value, query)));
                    }
                }
            }

            Task.WaitAll(tasks.ToArray());
        }
    }
}
