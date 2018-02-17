using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassLibrary1
{
    public class DBManager
    {

        public static HashSet<String> AddedLinks { get; set; } = new HashSet<string>();
        public static List<String> Blacklist = new List<string>();

        public static CloudQueue getUrlQueue()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                ConfigurationManager.AppSettings["StorageConnectionString2"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference("urls");
            queue.CreateIfNotExists();

            return queue;
        }

        public static CloudQueue getDataQueue()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                ConfigurationManager.AppSettings["StorageConnectionString2"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference("data");
            queue.CreateIfNotExists();

            return queue;
        }

        public static CloudQueue getStatusQueue()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                ConfigurationManager.AppSettings["StorageConnectionString2"]);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
            CloudQueue queue = queueClient.GetQueueReference("status");
            queue.CreateIfNotExists();

            return queue;
        }

        public static CloudTable getResultsTable()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                 ConfigurationManager.AppSettings["StorageConnectionString2"]);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("results");
            table.CreateIfNotExists();

            return table;
        }

        public static CloudTable getErrorsTable()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                 ConfigurationManager.AppSettings["StorageConnectionString2"]);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("errors");
            table.CreateIfNotExists();

            return table;
        }

        public static CloudTable getPerformanceTable()
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                 ConfigurationManager.AppSettings["StorageConnectionString2"]);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("performance");
            table.CreateIfNotExists();

            return table;
        }

        /*
        public static HashSet<String> getAddedList()
        {
            if (AddedLinks == null)
            {
                System.Diagnostics.Debug.WriteLine("Made New List!");
                AddedLinks = new HashSet<string>();
            }

            return AddedLinks;
        }

        public static void AddToList(string input)
        {
            try
            {
                AddedLinks.Add(input);
            }
            catch
            {
                AddedLinks = new HashSet<string>();
                AddedLinks.Add(input);
            }
        }
        */
        public DBManager() {
        }

    }
}
