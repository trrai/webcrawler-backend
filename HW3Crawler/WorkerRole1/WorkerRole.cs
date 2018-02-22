using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using ClassLibrary1;
using HtmlAgilityPack;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;

namespace WorkerRole1
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.TraceInformation("WorkerRole1 is running");

            while (true)
            {
                Trace.TraceInformation("Working");
                //only run if the user says so
                if (statusCheck())
                {

                    //URL queue holds the sitemaps
                    var message = DBManager.getUrlQueue().GetMessage();

                    //Data queue holds the html links
                    var dataMessage = DBManager.getDataQueue().GetMessage();

                    //there are sitemaps left to be processed
                    if (message != null)
                    {

                        string link = message.AsString;

                        processURL(link);
                        try
                        {
                            DBManager.getUrlQueue().DeleteMessage(message);
                        }
                        catch
                        {
                            System.Diagnostics.Debug.WriteLine("Could not delete");
                        }


                    }

                    //this means the sitemaps are done processing, move onto the html links
                    else if (dataMessage != null)
                    {

                        string link = dataMessage.AsString;

                        scanURL(link);
                    }

                    //queue has to be empty
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("!! Nothing in Queue... !!");
                    }

                    new Task(getPerformance).Start();
                }

                Thread.Sleep(50);
            }


        }

        // This method returns true if the passed url is a website that responds with a 200 status code
        // and does not time out
        private bool isRespondingWebsite(string url)
        {

            try
            {
                //System.Diagnostics.Debug.WriteLine("Checkpoint3: " + link);
                //Add checks for 200 code here
                HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(url);
                webRequest.AllowAutoRedirect = false;
                webRequest.Method = "HEAD";

                //website is valid
                using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                {

                    // Do your processings here....
                    var responseCode = response.StatusCode;
                    //System.Diagnostics.Debug.WriteLine("Response: " + responseCode);
                    response.Close();

                    if (responseCode == HttpStatusCode.OK)
                    {
                        return true;
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("<------ ERROR FOUND HERE ------>");
                        ClassLibrary1.Error newErr = new ClassLibrary1.Error(url, ("Error: Response Code " + responseCode + " on request!"));
                        TableOperation insertOperation = TableOperation.Insert(newErr);
                        DBManager.getErrorsTable().Execute(insertOperation);
                        System.Diagnostics.Debug.WriteLine("Error inserted: " + url);
                        return false;
                    }
                }
            }
            catch (Exception e)
            {
                System.Diagnostics.Debug.WriteLine("<------ ERROR FOUND HERE ------>");
                ClassLibrary1.Error newErr = new ClassLibrary1.Error(url, e.Message);
                TableOperation insertOperation = TableOperation.Insert(newErr);
                DBManager.getErrorsTable().Execute(insertOperation);
                System.Diagnostics.Debug.WriteLine("Error inserted: " + url);
                return false;
            }
        }

        // This method returns true if the passed url passes the rules of the robots.txt files and can 
        // be crawled 
        private bool notOnBlacklist(string url)
        {
            bool notBlacklisted = true;
            foreach (string blockedPath in DBManager.Blacklist)
            {
                if (url.Contains(blockedPath))
                {
                    notBlacklisted = false;
                }

            }
            return notBlacklisted;
        }

        // This method takes a url and parses the contents to pull html links that are held within. After
        // finding all valid href values within the website, it is sent to the results table
        public void scanURL(string url)
        {
            //initial checks for a 200 website + running status
            if (isRespondingWebsite(url) && statusCheck())
            {
                try
                {
                    //record performance
                    new Task(getPerformance).Start();

                    //Pull the desired elements from the html page using HTML AGILITY PACK
                    HtmlWeb hw = new HtmlWeb();
                    HtmlDocument doc = hw.Load(url);
                    
                    //loop through each link 
                    foreach (HtmlNode link in doc.DocumentNode.SelectNodes("//a[@href]"))
                    {
                        try
                        {

                            string hrefValue = link.Attributes["href"].Value;
                            Uri uriElement = new Uri(hrefValue);

                            //check for appropriate link structure
                            if (hrefValue.Contains("http") &&
                                (uriElement.Host.Contains("cnn.com") ||
                                uriElement.Host.Contains("bleacherreport.com/nba")))
                            {
                                //check if it's already been added
                                if (!DBManager.AddedLinks.Contains(hrefValue) && notOnBlacklist(hrefValue))
                                {
                                    System.Diagnostics.Debug.WriteLine(hrefValue);
                                    DBManager.AddedLinks.Add(hrefValue);
                                    System.Diagnostics.Debug.WriteLine("Added: " + DBManager.AddedLinks.Contains(hrefValue));
                                    CloudQueueMessage newLink = new CloudQueueMessage(hrefValue);
                                    DBManager.getDataQueue().AddMessage(newLink);
                                }

                            }

                        }
                        catch (Exception e)
                        {
                            System.Diagnostics.Debug.WriteLine("Found invalid url");
                            System.Diagnostics.Debug.WriteLine("Exception Message: " + e.Message);
                        }
                    }
                }
                catch (Exception e)
                {
                    System.Diagnostics.Debug.WriteLine("Fatal url error with" + url);
                    System.Diagnostics.Debug.WriteLine("Message:" + e.Message);
                }



                //This link is now done processing, send to the table!
                string title = " ";
                string publicationDate = " ";
                

                using (var client = new WebClient())
                {
                    try
                    {
                        //Find the publication date and title 
                        string linking = client.DownloadString(url);
                        HtmlDocument web = new HtmlDocument();
                        web.LoadHtml(linking);

                        //parse appropriate heading
                        title = web.DocumentNode.SelectSingleNode("//head/title").InnerText ?? "";
                        HtmlNode pubdate = web.DocumentNode.SelectSingleNode("//head/meta[@name='lastmod']");
                        if (pubdate != null)
                        {
                            //get publication date
                            publicationDate = DateTime.Parse(pubdate.Attributes["content"].Value).ToString();
                        }
                        else
                        {
                            //if there is no date, use today
                            publicationDate = DateTime.Today.ToString();
                        }

                    }
                    catch
                    {
                        System.Diagnostics.Debug.WriteLine("Could not find metadata for: " + url);
                    }
                }

                //Make the new website to be sent to the table
                Website newWebsite = new Website(title, url, publicationDate);

                //get current count
                int updatedCount = 1;
                TableOperation retrieve = TableOperation.Retrieve<Website>("COUNT", "COUNT");
                TableResult retrievedResult = DBManager.getResultsTable().Execute(retrieve);

                // Increment the count by 1 if applicable 
                if (retrievedResult.Result != null)
                {
                    int oldCount = ((Website)retrievedResult.Result).Count;

                    updatedCount = (int)oldCount + 1;

                }

                //Insert the new count and the new website
                TableOperation insertCount = TableOperation.InsertOrReplace(new Website(updatedCount));
                DBManager.getResultsTable().Execute(insertCount);

                TableOperation insertOperation = TableOperation.Insert(newWebsite);
                DBManager.getResultsTable().Execute(insertOperation);

            }

        }

        // This method takes a sitemap or robots.txt file and inserts all of the appropriate links that fit
        // the criteria to be parsed in the future
        public void processURL(string url)
        {
            //keep record of what to delete
            string deleteId = "";
            new Task(getPerformance).Start();

            //if we're allowed to run
            if (statusCheck())
            {
                //for robots.txt files
                if (url.Contains("robots.txt"))
                {
                    //in the case of bleacher report, we only add the nba sitemap
                    if (url.Contains("bleacherreport"))
                    {

                        CloudQueueMessage msg = new CloudQueueMessage("http://bleacherreport.com/sitemap/nba.xml");
                        deleteId = msg.Id;
                        DBManager.getUrlQueue().AddMessage(msg);
                    }
                    //otherwise go through the file and see what we can parse
                    else
                    {

                        Stream stream = (new WebClient()).OpenRead(url);
                        StreamReader reader = new StreamReader(stream);

                        //find site maps and disallow rules
                        while (!reader.EndOfStream)
                        {
                            string line = reader.ReadLine();
                            if (line.StartsWith("Sitemap:"))
                            {
                                line = line.Replace("Sitemap: ", "");
                                if (!DBManager.AddedLinks.Contains(line))
                                {
                                    DBManager.AddedLinks.Add(line);

                                    CloudQueueMessage msg = new CloudQueueMessage(line);
                                    deleteId = msg.Id;

                                    DBManager.getUrlQueue().AddMessageAsync(msg);
                                }
                            }
                            else if (line.StartsWith("Disallow: "))
                            {
                                line = line.Replace("Disallow: ", "");
                                DBManager.Blacklist.Add(line);
                            }


                        }
                    }
                }
                //if we're passed an xml link (a sitemap)
                else if (url.Contains(".xml"))
                {
                    //load the sitemap
                    XElement sitemap = XElement.Load(url);

                    //loop through sitemap
                    foreach (var element in sitemap.Elements())
                    {
                        //check status before continuing 
                        if (statusCheck())
                        {
                           
                            try
                            {
                                string link = element.Element(XName.Get("loc", "http://www.sitemaps.org/schemas/sitemap/0.9")).Value;

                                if (!DBManager.AddedLinks.Contains(link))
                                {
                                    DBManager.AddedLinks.Add(link);

                                    // if we get another sitemap
                                    if (link.Contains(".xml") && link.Contains("2018"))
                                    {
                                      
                                        CloudQueueMessage msg = new CloudQueueMessage(link);
                                        DBManager.getUrlQueue().AddMessage(msg);
                                    }

                                    // if we get other link type
                                    else if (!link.Contains(".xml"))
                                    {
                                        //use a variety of elements and maps to parse links and publication dates
                                        string publishedOn = "";

                                        if (element.Element(XName.Get("news", "http://www.google.com/schemas/sitemap-news/0.9")) != null)
                                        {

                                            var newsElement = element.Element(XName.Get("news", "http://www.google.com/schemas/sitemap-news/0.9"));
                                            publishedOn = newsElement.Element(XName.Get("publication_date", "http://www.google.com/schemas/sitemap-news/0.9")).Value;
                                        }
                                        else if (element.Element(XName.Get("video", "http://www.google.com/schemas/sitemap-video/1.1")) != null)
                                        {

                                            var videoElement = element.Element(XName.Get("video", "http://www.google.com/schemas/sitemap-video/1.1"));
                                            publishedOn = videoElement.Element(XName.Get("publication_date", "http://www.google.com/schemas/sitemap-video/1.1")).Value;
                                        }
                                        else if (element.Element(XName.Get("lastmod", "http://www.sitemaps.org/schemas/sitemap/0.9")) != null)
                                        {

                                            publishedOn = element.Element(XName.Get("lastmod", "http://www.sitemaps.org/schemas/sitemap/0.9")).Value;
                                        }
                                        else
                                        {

                                            publishedOn = new DateTime(2018, 10, 2).ToString();
                                        }

                                        //make variable to designate 2 months ago
                                        DateTime twoMonthsAgo = DateTime.Today.AddDays(-60);

                                        //check if link is recent enough
                                        if (DateTime.Compare(DateTime.Parse(publishedOn), twoMonthsAgo) > 0)
                                        {

                                            // check if it's an appropriate html link
                                            if (link.Contains(".html") || link.Contains(".htm"))
                                            {
                                                if (isRespondingWebsite(link))
                                                {

                                                    CloudQueueMessage nMsg = new CloudQueueMessage(link);
                                                    DBManager.getDataQueue().AddMessageAsync(nMsg);

                                                    //sleep before continuing 
                                                    Thread.Sleep(100);
                                                }

                                            }
                                        }

                                    }
                                }
                            }
                            catch (Exception e)
                            {

                                //try the other sitemap format that works for bleacherreport
                                try
                                {
                                    string link = element.Element(XName.Get("loc", "http://www.google.com/schemas/sitemap/0.9")).Value;
                                    System.Diagnostics.Debug.WriteLine("Link Parsed: " + link);
                                }
                                catch
                                {
                                    System.Diagnostics.Debug.WriteLine("Not a supported link..");
                                }

                            }
                        }

                    }
                }
            }

        }

        // This method monitors and logs performance values like processor % and memory available on the instance
        public void getPerformance()
        {
            PerformanceCounter theCPUCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total");
            PerformanceCounter theMemCounter = new PerformanceCounter("Memory", "Available MBytes");
            theCPUCounter.NextValue();
            Thread.Sleep(500);
            int currentCPU = (int)theCPUCounter.NextValue();
            int currentMem = (int)theMemCounter.NextValue();

            PerformanceStat newStat = new PerformanceStat(currentCPU, currentMem);
            TableOperation insertOperation = TableOperation.Insert(newStat);
            DBManager.getPerformanceTable().Execute(insertOperation);
        }

        // This method checks if the status queue holds a start or stop message, and conveys the message to whether
        // or not keep running. 
        private bool statusCheck()
        {
            CloudQueueMessage msg = DBManager.getStatusQueue().PeekMessage();

            if (msg != null)
            {


                if (msg.AsString.Equals("Stopped"))
                {

                    return false;
                }
                else if (msg.AsString.Equals("Started"))
                {

                    return true;
                }

            }

            return false;
        }
    }
}
