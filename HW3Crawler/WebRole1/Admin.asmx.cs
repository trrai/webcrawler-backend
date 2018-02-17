using ClassLibrary1;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
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


        [WebMethod]
        public string StopCrawl()
        {
            UpdateStatus("Stopped");
            return "Stopped";
        }

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
        private void UpdateStatus(string newStatus)
        {
            CloudQueueMessage msg = DBManager.getStatusQueue().PeekMessage();
            try
            {
                System.Diagnostics.Debug.WriteLine("Msg content: " + msg);
                System.Diagnostics.Debug.WriteLine("Found message in ASMX: " + msg.AsString);
            }
            catch { }
            
            if (msg != null)
            { 
                System.Diagnostics.Debug.WriteLine("Found message in ASMX: " + msg.AsString);
                DBManager.getStatusQueue().Clear();
            }
            

            System.Diagnostics.Debug.WriteLine("Msg found to be null");
            System.Diagnostics.Debug.WriteLine(newStatus);
            CloudQueueMessage nStatus = new CloudQueueMessage(newStatus);
            DBManager.getStatusQueue().AddMessage(nStatus);
            System.Diagnostics.Debug.WriteLine("Updated");
        }

        [WebMethod]
        public string ClearUrlQueue()
        {
            DBManager.getUrlQueue().Clear();
            return "Cleared";
        }

        [WebMethod]
        [ScriptMethod(ResponseFormat = ResponseFormat.Json)]
        public string GetCrawledUrls()
        {
            DBManager.getDataQueue().FetchAttributes();
            var count = DBManager.getDataQueue().ApproximateMessageCount;
            return count.ToString();

        }

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


            System.Diagnostics.Debug.WriteLine("===== LIST =====");
            foreach (var item in q.Take(10))
            {
                System.Diagnostics.Debug.WriteLine(item.Address);
                returnList.Add(item.Address);
            }
            return returnList;

        }

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


            System.Diagnostics.Debug.WriteLine("===== PERFORMANCE LIST =====");
            foreach (var item in q.Take(10))
            {
                System.Diagnostics.Debug.WriteLine("CPU: " + item.CPU + " --- Memory: " + item.Memory);
                returnList.Add("CPU: " + item.CPU.ToString() + " --- Memory: " + item.Memory.ToString());
            }
            return returnList;
        }
    }
}
