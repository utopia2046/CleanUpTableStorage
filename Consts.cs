using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CleanUpProdTables
{
    class Consts
    {
        public const string DateFormat = "MM-dd-yyyy HH:mm:ss";
        public const string FilterStringFormat = "PartitionKey lt '<dateString>'";
        public const int BatchSize = 100;
        public const int RetentionDays = 60;

        public static string[] TableNames = { "Error", "ApplicationError", "UserError", "Latency", "Performance", "Trace" };

        private static List<Account> _accounts = null;
        public static List<Account> Accounts
        {
            get
            {
                if (_accounts == null)
                {
                    _accounts = new List<Account>();
                    _accounts.Add(new Account("", "DefaultEndpointsProtocol=https;AccountName=;AccountKey="));
                    _accounts.Add(new Account("", "DefaultEndpointsProtocol=https;AccountName=;AccountKey="));
                    _accounts.Add(new Account("", "DefaultEndpointsProtocol=https;AccountName=;AccountKey="));
                    _accounts.Add(new Account("", "DefaultEndpointsProtocol=https;AccountName=;AccountKey="));
                    _accounts.Add(new Account("", "DefaultEndpointsProtocol=https;AccountName=;AccountKey="));
                }

                return _accounts;
            }
        }
    }

    struct Account
    {
        public string Name;
        public string Key;
        public Account(string name, string key)
        {
            Name = name;
            Key = key;
        }
    }
}
