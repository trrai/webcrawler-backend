using ClassLibrary1;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Web;
using System.Web.Script.Serialization;
using System.Web.Script.Services;
using System.Web.Services;

namespace WebRole1
{
    /// <summary>
    /// Summary description for Admin
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    [System.Web.Script.Services.ScriptService]
    public class Admin : System.Web.Services.WebService
    {
        

        // Method to begin the crawling process, or resume if stopped
        [WebMethod]
        public string StartCrawl()
        {
            
            CloudQueueMessage msg = DBManager.getStatusQueue().PeekMessage();

            if (msg == null)
            {
                
                CloudQueueMessage cnnMsg = new CloudQueueMessage("http://cnn.com/robots.txt");
                DBManager.getUrlQueue().AddMessage(cnnMsg);
                
                CloudQueueMessage brMsg = new CloudQueueMessage("http://bleacherreport.com/robots.txt");
                DBManager.getUrlQueue().AddMessage(brMsg);

            }

            UpdateStatus("Started");
            return "Started";
        }

        // Method to stop the crawling process if it is underway
        [WebMethod]
        public string StopCrawl()
        {
            UpdateStatus("Stopped");
            return "Stopped";
        }

        // Returns the current status of the crawler
        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string GetStatus()
        {
            CloudQueueMessage msg = DBManager.getStatusQueue().PeekMessage();
            if (msg != null)
            {
                return new JavaScriptSerializer().Serialize(msg.AsString);
            }
            else
            {
                return new JavaScriptSerializer().Serialize("Idle");
            }
        }

        // Updates the status by changing the message in the queue
        private void UpdateStatus(string newStatus)
        {
            CloudQueueMessage msg = DBManager.getStatusQueue().PeekMessage();
            
            if (msg != null)
            { 
                //System.Diagnostics.Debug.WriteLine("Found message in ASMX: " + msg.AsString);
                DBManager.getStatusQueue().Clear();
            }
            

            //System.Diagnostics.Debug.WriteLine("Msg found to be null");
            System.Diagnostics.Debug.WriteLine(newStatus);
            CloudQueueMessage nStatus = new CloudQueueMessage(newStatus);
            DBManager.getStatusQueue().AddMessage(nStatus);
            System.Diagnostics.Debug.WriteLine("Updated");
        }

        // Clears the url queue's content
        [WebMethod]
        public string ClearUrlQueue()
        {
            DBManager.getUrlQueue().Clear();
            return "Cleared";
        }

        // Returns the current count of the queue and table separated by a pipe symbol
        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string GetCount()
        {
            CloudQueue q = DBManager.getDataQueue();
            q.FetchAttributes();
            var qCnt = q.ApproximateMessageCount;

            TableOperation retrieve = TableOperation.Retrieve<Website>("COUNT", "COUNT");

            TableResult retrievedResult = DBManager.getResultsTable().Execute(retrieve);

            int tableCount = 0;
            // Print the phone number of the result.
            if (retrievedResult.Result != null)
            {
                int currentCount = ((Website)retrievedResult.Result).Count;
                tableCount = (int)currentCount;
            }
            else
            {
                //System.Diagnostics.Debug.WriteLine("Failed to retrieve");
            }

            return new JavaScriptSerializer().Serialize(qCnt.ToString() + "|" + tableCount.ToString()); 

        }

        // Returns a JSON formatted list of the last 10 links added to the results table
        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public List<String> GetLast10Added()
        {

            TableQuery<Website> rangeQuery = new TableQuery<Website>()
                .Where(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.NotEqual, " ")
                    );

            List<String> returnList = new List<String>();

            var q = DBManager.getResultsTable().ExecuteQuery(rangeQuery);


            //System.Diagnostics.Debug.WriteLine("===== LIST =====");
            foreach (var item in q.Take(10))
            {
                //System.Diagnostics.Debug.WriteLine(item.Address);
                returnList.Add(item.Address);
            }
            return returnList;

        }

        // Method to get the last 10 performance entries being recorded by the worker role
        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public List<String> GetPerformance()
        {
            TableQuery<PerformanceStat> rangeQuery = new TableQuery<PerformanceStat>()
                .Where(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.NotEqual, " ")
                    );

            List<String> returnList = new List<String>();

            var q = DBManager.getPerformanceTable().ExecuteQuery(rangeQuery);


            //System.Diagnostics.Debug.WriteLine("===== PERFORMANCE LIST =====");
            foreach (var item in q.Take(10))
            {
                //System.Diagnostics.Debug.WriteLine("CPU: " + item.CPU + " --- Memory: " + item.Memory);
                returnList.Add("CPU: " + item.CPU.ToString() + " --- Memory: " + item.Memory.ToString());
            }
            return returnList;
        }

        // Method to get the performance data that is specially formatted to create the chart
        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public List<String> GetPerformanceChartData()
        {
            TableQuery<PerformanceStat> rangeQuery = new TableQuery<PerformanceStat>()
                .Where(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.NotEqual, " ")
                    );

            List<String> returnList = new List<String>();

            var q = DBManager.getPerformanceTable().ExecuteQuery(rangeQuery);


            //System.Diagnostics.Debug.WriteLine("===== PERFORMANCE LIST =====");
            foreach (var item in q.Take(10))
            {
                //System.Diagnostics.Debug.WriteLine("CPU: " + item.CPU + " --- Memory: " + item.Memory);
                returnList.Add(item.CPU.ToString() + "|" + item.Memory.ToString());
            }
            return returnList;
        }

        // Returns the last 10 errors recorded by the worker role when processing links
        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public List<String> GetErrors()
        {
            TableQuery<Error> rangeQuery = new TableQuery<Error>()
                .Where(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.NotEqual, " ")
                    );

            List<String> returnList = new List<String>();

            var q = DBManager.getErrorsTable().ExecuteQuery(rangeQuery);


            //System.Diagnostics.Debug.WriteLine("===== ERROR LIST =====");
            foreach (var item in q.Take(10))
            {
                //System.Diagnostics.Debug.WriteLine("CPU: " + item.CPU + " --- Memory: " + item.Memory);
                returnList.Add(item.Link.ToString() + " | "+ item.ErrorMsg.ToString());
            }
            return returnList;
        }
    }
}
