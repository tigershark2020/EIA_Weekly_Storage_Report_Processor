using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;

namespace EIA_Weekly_Storage_Report_Processor
{
    class Program
    {
        public struct ConfigFiles
        {
            public string mysqlCredentialsSelect_JsonFile { get; set; }
            public string mysqlCredentialsInsert_JsonFile { get; set; }
            public string userSites_JsonFile { get; set; }
        }

        public class Event_Notification
        {
            public string eventNotification_Agency { get; set; }
            public string eventNotification_Title { get; set; }
            public string eventNotification_URL { get; set; }
            public long eventNotification_DatetimeEpoch { get; set; }
            public string eventNotification_Category { get; set; }
            public string eventNotification_Type { get; set; }
            public string eventNotification_UniqueID { get; set; }
            public double eventNotification_Latitude { get; set; }
            public double eventNotification_Longitude { get; set; }
        }

        public class EIA_Storage_Data
        {
            public string ReportStub {get; set;}
            public string CurrentWeek { get; set; }
            public string WeekAgo { get; set; }
            public string WeekAgo_Difference { get; set; }
            public string WeekAgo_PercentageChange {get; set;}
            public string YearAgo { get; set; }
            public string YearAgo_DifferenceAmount { get; set; }
            public string YearAgo_PercentChange { get; set; }
        }

        public class EIA_Storage_Data_Values
        {
            public string ReportStub { get; set; }
            public string CurrentWeek { get; set; }
            public string WeekAgo { get; set; }
            public double WeekAgo_Difference { get; set; }
            public double WeekAgo_PercentageChange { get; set; }
            public string YearAgo { get; set; }
            public double YearAgo_DifferenceAmount { get; set; }
            public double YearAgo_PercentChange { get; set; }
        }

        public class EIA_Storage_Data_Report
        {
            public EIA_Storage_Data Header_Data { get; set; }
            public List<EIA_Storage_Data_Values> Values { get; set; }
        }

        public static string Download_Report_Text(string url)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            HttpWebResponse response = (HttpWebResponse)request.GetResponse();

            StreamReader sr = new StreamReader(response.GetResponseStream());
            string results = sr.ReadToEnd();
            sr.Close();

            return results;
        }

        public static void Add_Event_Notifications(ConfigFiles jsonConfigPaths, List<Event_Notification> eventNotificationList)
        {

            MySql.Data.MySqlClient.MySqlConnection conn;

            conn = new MySql.Data.MySqlClient.MySqlConnection();

            MySqlConnectionStringBuilder conn_string_builder = new MySqlConnectionStringBuilder();
            string json = System.IO.File.ReadAllText(jsonConfigPaths.mysqlCredentialsInsert_JsonFile);
            conn_string_builder = JsonConvert.DeserializeObject<MySqlConnectionStringBuilder>(json);

            conn = new MySqlConnection(conn_string_builder.ToString());
            try
            {
                conn.Open();
            }
            catch (Exception erro)
            {
                Console.WriteLine(erro);
            }

            foreach (Event_Notification eventNotification in eventNotificationList)
            {
                try
                {
                    MySqlCommand cmd = conn.CreateCommand();
                    cmd.Connection = conn;

                    cmd.CommandText = "INSERT INTO `geo_data`.`geo_events` (`geo_event_agency`,`geo_event_title`,`geo_event_url`,`geo_event_starttime`,`geo_event_category`,`geo_event_type`,`geo_event_ident`,`geo_event_location_latitude`,`geo_event_location_longitude`,`geo_event_notify`) VALUES (@event_notification_agency,@event_notification_title,@event_notification_url,FROM_UNIXTIME(@event_notification_datetime),@event_notification_category,@event_notification_type,@event_notification_ident,@event_notification_latitude,@event_notification_longitude,1);";
                    // cmd.Prepare();

                    cmd.Parameters.AddWithValue("@event_notification_agency", eventNotification.eventNotification_Agency);
                    cmd.Parameters.AddWithValue("@event_notification_title", eventNotification.eventNotification_Title);
                    cmd.Parameters.AddWithValue("@event_notification_url", eventNotification.eventNotification_URL);
                    cmd.Parameters.AddWithValue("@event_notification_datetime", eventNotification.eventNotification_DatetimeEpoch);
                    cmd.Parameters.AddWithValue("@event_notification_category", eventNotification.eventNotification_Category);
                    cmd.Parameters.AddWithValue("@event_notification_type", eventNotification.eventNotification_Type);
                    cmd.Parameters.AddWithValue("@event_notification_ident", eventNotification.eventNotification_UniqueID);
                    cmd.Parameters.AddWithValue("@event_notification_latitude", eventNotification.eventNotification_Latitude);
                    cmd.Parameters.AddWithValue("@event_notification_longitude", eventNotification.eventNotification_Longitude);

                    cmd.ExecuteNonQuery();
                }
                catch (MySqlException error_message)
                {
                    int errorcode = error_message.Number;
                    Console.WriteLine(errorcode + "\t" + error_message.Message);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
            }

            conn.Close();

        }

        public static void Process_Storage_Report()
        {
            EIA_Storage_Data_Report StorageReport = new EIA_Storage_Data_Report();
            string url = "https://ir.eia.gov/wpsr/table1.csv";

            string reportText = Download_Report_Text(url);
            string[] reportLinesArray;

            reportLinesArray = reportText.Split('\n');

            int counter = 0;
            bool continue_status = true;

            List<EIA_Storage_Data_Values> dataList = new List<EIA_Storage_Data_Values>();

            foreach (string item in reportLinesArray)
            {
                if(continue_status == true)
                {
                    if (counter > 0)
                    {
                        String[] dataArray = item.Split("\",\"");

                        string reportStubString = dataArray[0].Replace("\"", "");
 
                        if (reportStubString.Replace("\"", "").Contains("STUB_1"))
                        {
                            continue_status = false;
                        }
                        else
                        {
                            EIA_Storage_Data_Values dataItem = new EIA_Storage_Data_Values();
                            dataItem.ReportStub = reportStubString;

                            dataItem.CurrentWeek = dataArray[1].Replace("\"", "");
                            dataItem.WeekAgo = dataArray[2].Replace("\"", "");
                            dataItem.WeekAgo_Difference = Double.Parse(dataArray[3].Replace("\"", "").Replace(",", "").Trim());
                            dataItem.WeekAgo_PercentageChange = Double.Parse(dataArray[4].Replace("\"", "").Replace(",", "").Trim());
                            dataItem.YearAgo = dataArray[5].Replace("\"", "");
                            dataItem.YearAgo_DifferenceAmount = Double.Parse(dataArray[6].Replace("\"", "").Replace(",", "").Trim());
                            dataItem.YearAgo_PercentChange = Double.Parse(dataArray[7].Replace("\"", "").Replace(",", "").Trim());

                            dataList.Add(dataItem);
                            StorageReport.Values = dataList;
                        }



                    }
                    else
                    {
                        String[] headerArray = item.Split("\",\"");

                        EIA_Storage_Data header = new EIA_Storage_Data();
                        header.ReportStub = headerArray[0].Replace("\"", "");
                        header.CurrentWeek = headerArray[1].Replace("\"", "");
                        header.WeekAgo = headerArray[2].Replace("\"", "");
                        header.WeekAgo_Difference = headerArray[3].Replace("\"", "");
                        header.WeekAgo_PercentageChange = headerArray[4].Replace("\"", "");
                        header.YearAgo = headerArray[5].Replace("\"", "");
                        header.YearAgo_DifferenceAmount = headerArray[6].Replace("\"", "");
                        header.YearAgo_PercentChange = headerArray[7].Replace("\"", "");

                        StorageReport.Header_Data = header;
                    }
                }
                counter++;
            }

            List<Event_Notification> notifications = new List<Event_Notification>();

            Console.WriteLine("Weekly Report\t " + StorageReport.Header_Data.WeekAgo_Difference + "\t" + StorageReport.Header_Data.WeekAgo_PercentageChange);

            foreach (EIA_Storage_Data_Values storageColumnData in StorageReport.Values)
            {
                if (storageColumnData.ReportStub.Equals("Crude Oil"))
                {
                    Event_Notification energyNotification = new Event_Notification();
                    string title;
                    if (storageColumnData.WeekAgo_Difference < 0)
                    {
                        title = "Storage Drop Week Over Week";
                        Console.WriteLine(title);
                    }
                    else
                    {
                        title = "Storage Gain Week Over Week";
                        Console.WriteLine(title);
                    }

                    long unixTimestamp = (long)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
                    energyNotification.eventNotification_Agency = "48970";
                    energyNotification.eventNotification_Title = title;
                    energyNotification.eventNotification_DatetimeEpoch = unixTimestamp;
                    energyNotification.eventNotification_Latitude = 35.9860348;
                    energyNotification.eventNotification_Longitude = -96.7892684;
                    energyNotification.eventNotification_Category = "Energy";
                    energyNotification.eventNotification_Type = "Economic Data";
                    notifications.Add(energyNotification);
                    Console.WriteLine(storageColumnData.ReportStub + "\t" + storageColumnData.WeekAgo_Difference + "\t" + storageColumnData.WeekAgo_PercentageChange);
                }
            }

            string configFilePaths = "filePaths.json";
            bool exists = File.Exists(configFilePaths);
            string json = null;

            try
            {
                json = System.IO.File.ReadAllText(configFilePaths, System.Text.Encoding.UTF8);
            }
            catch (Exception json_read)
            {
                Console.WriteLine(json_read.Message);
            }

            if (json != null) // Check That JSON String Read Above From File Contains Data
            {
                ConfigFiles jsonConfigPaths = new ConfigFiles();
                jsonConfigPaths = JsonConvert.DeserializeObject<ConfigFiles>(json);
                Add_Event_Notifications(jsonConfigPaths, notifications);
            }

        }

        static void Main(string[] args)
        {
            Process_Storage_Report();
        }
    }
}
