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
                if (statusCheck())
                {
                    System.Diagnostics.Debug.WriteLine("Working...");
                    var message = DBManager.getUrlQueue().GetMessage();
                    var dataMessage = DBManager.getDataQueue().GetMessage();

                    if (message != null)
                    {
                        System.Diagnostics.Debug.WriteLine("!! XML SCAN PHASE !!");
                        string link = message.AsString;
                        System.Diagnostics.Debug.WriteLine("Process running with: " + link);
                        processURL(link);
                        try
                        {
                            DBManager.getUrlQueue().DeleteMessage(message);
                        }
                        catch
                        {
                            System.Diagnostics.Debug.WriteLine("Could not delete");
                        }
                        System.Diagnostics.Debug.WriteLine("Deleted :" + link);

                    }
                    else if (dataMessage != null)
                    {
                        System.Diagnostics.Debug.WriteLine("!! URL SCAN PHASE !!");
                        string link = dataMessage.AsString;
                        System.Diagnostics.Debug.WriteLine("Scan running with: " + link);
                        scanURL(link);
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("!! Nothing in Queue... !!");
                    }

                    new Task(getPerformance).Start();
                }

                Thread.Sleep(50);
            }


        }

        private bool isRespondingweb(string url)
        {

            try
            {
                //System.Diagnostics.Debug.WriteLine("Checkpoint3: " + link);
                //Add checks for 200 code here
                HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(url);
                webRequest.AllowAutoRedirect = false;

                //website is valid
                using (HttpWebResponse response = (HttpWebResponse)webRequest.GetResponse())
                {

                    // Do your processings here....
                    int responseCode = (int)response.StatusCode;
                    //System.Diagnostics.Debug.WriteLine("Response: " + responseCode);
                    if (responseCode == 200)
                    {
                        return true;
                    }
                    else
                    {
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
                ClassLibrary1.Error newErr = new ClassLibrary1.Error(url, e.Message);
                TableOperation insertOperation = TableOperation.Insert(newErr);
                DBManager.getErrorsTable().Execute(insertOperation);
                System.Diagnostics.Debug.WriteLine("Error inserted: " + url);
                return false;
            }
        }

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

        public void scanURL(string url)
        {
            if (isRespondingweb(url) && statusCheck())
            {
                try
                {
                    new Task(getPerformance).Start();
                    System.Diagnostics.Debug.WriteLine(url + " was found to be valid! Parsing...");
                    HtmlWeb hw = new HtmlWeb();
                    HtmlDocument doc = hw.Load(url);
                    foreach (HtmlNode link in doc.DocumentNode.SelectNodes("//a[@href]"))
                    {
                        try
                        {

                            string hrefValue = link.Attributes["href"].Value;
                            Uri uriElement = new Uri(hrefValue);
                            //TODO: MAKE SURE YOU ADD ONLY NBA FILTER FOR BLEACHER REPORTS
                            if (hrefValue.Contains("http") &&
                                (uriElement.Host.Contains("cnn.com") ||
                                uriElement.Host.Contains("bleacherreport.com/nba")))
                            {
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
                            System.Diagnostics.Debug.WriteLine("AddedLinks Count: " + DBManager.AddedLinks.Count);

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
                        string linking = client.DownloadString(url);
                        HtmlDocument web = new HtmlDocument();
                        web.LoadHtml(linking);

                        title = web.DocumentNode.SelectSingleNode("//head/title").InnerText ?? "";
                        HtmlNode pubdate = web.DocumentNode.SelectSingleNode("//head/meta[@name='lastmod']");
                        if (pubdate != null)
                        {
                            publicationDate = DateTime.Parse(pubdate.Attributes["content"].Value).ToString();
                        }
                        else
                        {
                            publicationDate = DateTime.Today.ToString();
                        }

                    }
                    catch
                    {
                        System.Diagnostics.Debug.WriteLine("Could not find metadata for: " + url);
                    }
                }

                //System.Diagnostics.Debug.WriteLine("Title: " + title);
                //System.Diagnostics.Debug.WriteLine("Url: " + url);
                //System.Diagnostics.Debug.WriteLine("Pub Date: " + publicationDate);

                Website newWebsite = new Website(title, url, publicationDate);
                TableOperation insertOperation = TableOperation.Insert(newWebsite);
                DBManager.getResultsTable().Execute(insertOperation);



            }

        }

        public void processURL(string url)
        {
            string deleteId = "";
            new Task(getPerformance).Start();
            if (statusCheck())
            {
                if (url.Contains("robots.txt"))
                {

                    if (url.Contains("bleacherreport"))
                    {
                        System.Diagnostics.Debug.WriteLine("Just added bleacherreport link!");
                        CloudQueueMessage msg = new CloudQueueMessage("http://bleacherreport.com/sitemap/nba.xml");
                        deleteId = msg.Id;
                        DBManager.getUrlQueue().AddMessage(msg);
                    }
                    else
                    {
                        System.Diagnostics.Debug.WriteLine("Checkpoint3");
                        Stream stream = (new WebClient()).OpenRead(url);
                        StreamReader reader = new StreamReader(stream);

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
                                    System.Diagnostics.Debug.WriteLine("Just added: " + line + " from robots.txt!");
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
                else if (url.Contains(".xml"))
                {
                    System.Diagnostics.Debug.WriteLine("Reached xml section!");
                    XElement sitemap = XElement.Load(url);
                    foreach (var element in sitemap.Elements())
                    {
                        if (statusCheck())
                        {
                            try
                            {
                                string link = element.Element(XName.Get("loc", "http://www.sitemaps.org/schemas/sitemap/0.9")).Value;

                                if (!DBManager.AddedLinks.Contains(link))
                                {
                                    DBManager.AddedLinks.Add(link);

                                    if (link.Contains(".xml") && link.Contains("2018"))
                                    {
                                        CloudQueueMessage msg = new CloudQueueMessage(link);
                                        DBManager.getUrlQueue().AddMessage(msg);
                                    }
                                    else if (!link.Contains(".xml"))
                                    {

                                        string publishedOn = "";

                                        if (element.Element(XName.Get("news", "http://www.google.com/schemas/sitemap-news/0.9")) != null)
                                        {
                                            //System.Diagnostics.Debug.WriteLine("News source found");
                                            var newsElement = element.Element(XName.Get("news", "http://www.google.com/schemas/sitemap-news/0.9"));
                                            publishedOn = newsElement.Element(XName.Get("publication_date", "http://www.google.com/schemas/sitemap-news/0.9")).Value;
                                        }
                                        else if (element.Element(XName.Get("video", "http://www.google.com/schemas/sitemap-video/1.1")) != null)
                                        {
                                            //System.Diagnostics.Debug.WriteLine("Video source found");
                                            var videoElement = element.Element(XName.Get("video", "http://www.google.com/schemas/sitemap-video/1.1"));
                                            publishedOn = videoElement.Element(XName.Get("publication_date", "http://www.google.com/schemas/sitemap-video/1.1")).Value;
                                        }
                                        else if (element.Element(XName.Get("lastmod", "http://www.sitemaps.org/schemas/sitemap/0.9")) != null)
                                        {
                                            ///System.Diagnostics.Debug.WriteLine("Other source found");
                                            publishedOn = element.Element(XName.Get("lastmod", "http://www.sitemaps.org/schemas/sitemap/0.9")).Value;
                                        }
                                        else
                                        {
                                            //System.Diagnostics.Debug.WriteLine("Else Branch source found");
                                            publishedOn = new DateTime(2018, 10, 2).ToString();
                                        }

                                        //System.Diagnostics.Debug.WriteLine("Publication date: " + publishedOn);
                                        DateTime twoMonthsAgo = DateTime.Today.AddDays(-60);

                                        //System.Diagnostics.Debug.WriteLine("Checkpoint1: " + link);
                                        if (DateTime.Compare(DateTime.Parse(publishedOn), twoMonthsAgo) > 0)
                                        {
                                            //System.Diagnostics.Debug.WriteLine("Checkpoint2: " + link);
                                            //Add checks for ending in html here
                                            if (link.Contains(".html") || link.Contains(".htm"))
                                            {
                                                if (isRespondingweb(link))
                                                {
                                                    // System.Diagnostics.Debug.WriteLine("Checkpoint4: " + link);
                                                    //System.Diagnostics.Debug.WriteLine("Link Parsed: " + link + " published on " + publishedOn);
                                                    CloudQueueMessage nMsg = new CloudQueueMessage(link);
                                                    DBManager.getDataQueue().AddMessageAsync(nMsg);

                                                    Thread.Sleep(100);
                                                }

                                            }
                                        }

                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                System.Diagnostics.Debug.WriteLine("Exception: " + e);
                                System.Diagnostics.Debug.WriteLine("Url: " + url);
                                try
                                {
                                    string link = element.Element(XName.Get("loc", "http://www.google.com/schemas/sitemap/0.9")).Value;
                                    System.Diagnostics.Debug.WriteLine("Link Parsed: " + link);
                                }
                                catch
                                {
                                    System.Diagnostics.Debug.WriteLine("Not a bleacer report link..");
                                }

                            }
                        }

                    }
                }
            }

        }

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

        private bool statusCheck()
        {
            CloudQueueMessage msg = DBManager.getStatusQueue().PeekMessage();

            if (msg != null)
            {

                System.Diagnostics.Debug.WriteLine("Message Found :" + msg.AsString);
                if (msg.AsString.Equals("Stopped"))
                {
                    System.Diagnostics.Debug.WriteLine("<------ STOPPED -------->");
                    return false;
                }
                else if (msg.AsString.Equals("Started"))
                {
                    System.Diagnostics.Debug.WriteLine("<------ STARTED -------->");
                    return true;
                }

            }

            return false;
        }
    }
}
