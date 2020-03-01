using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace OrangeCity_Console
{
    class Program
    {
        public static List<string> setup = new List<string>();
        public static List<string> TotDate = new List<string>();
        public static List<string> locs = new List<string>();
        public static string LocalDB, LocalNgpDB, line, path, logpath, typeprocess, Location, TotalFlowUnitMk, FlowUnitMk, RemoteDB, LogQuery, Key, SNO, from, to = "";
        public static string LogSerialNo, LogSIMNo;
        public static SqlConnection LocalDBconn = new SqlConnection();
        public static SqlConnection NGPDBconn = new SqlConnection();
        public static SqlConnection LocalNgpDBconn = new SqlConnection();
        //public static SqlCommand insertPressure;
        //public static SqlCommand insertFlow;
        public static List<LocationsID> results;
        //public static string[] allfiles;
        public static Int32 i, j = 1, k = 1, actsite = 0;
        public static decimal FlowRate, ActFlowRate, Consumption, Lat, Long, PressureValue, LastPeriod, CurrentPeriod = 0;
        public static double FlowUnit, TotalFlowUnit;
        //public static ConsoleKeyInfo cki;
        public static DateTime From_Date, To_Date;


        static void Main(string[] args)
        {
            path = "C:\\HWM\\SCADA\\SCADA.txt";
            var dateAndTime = DateTime.Now;
            var date = dateAndTime.ToShortDateString();
            logpath = "C:\\HWM\\SCADA\\Log\\" + date + ".txt";

            if (!File.Exists(path))
            {
                string[] lines = { "Title-", "Server-", "Database-", "UserName-", "Password-", "Local-", "Remote-", "Error-", "Mode (A/M)-", "Totalizer Units (m3/Lts)-", "Flow Rate (Lts.s/m3.h)-", "Totalizer Correction-", "Flow rate Correction-", "Interval (hrs)-", "Interval (Mins)-", "Channel No (Flow)-", "License No-", "License Status (Y/N/NA)-NA", "Transfer (FTP/Local)-", "FTP IP-", "FTP/WIndows Username-", "FTP/Windows Password-", "Delete (Y/N)-", "Local 2-" };

                System.IO.File.WriteAllLines(path, lines);
            }

            System.IO.StreamReader file =
              new StreamReader(path);
            while ((line = file.ReadLine()) != null)
            {
                int index = line.LastIndexOf("-");
                if (index > 0)
                    line = line.Substring(index + 1);
                if (!String.IsNullOrWhiteSpace(line))
                {
                    setup.Add(line);
                }
                else
                { j = 0; }

                //call your function here  
            }
            if (j != 0)
            {

                LocalDB = "Data Source=" + setup[1] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[2] + ";Persist Security Info=True;User ID=" + setup[3] + ";Password=" + setup[4] + "";
                Console.Title = setup[0];
                RemoteDB = "Data Source=" + setup[5] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[6] + ";Persist Security Info=True;User ID=" + setup[7] + ";Password=" + setup[8] + "";
                LocalNgpDB = "Data Source=" + setup[5] + ";MultipleActiveResultSets=true;Initial Catalog=" + setup[19] + ";Persist Security Info=True;User ID=" + setup[7] + ";Password=" + setup[8] + "";
                StartProcess();
            }
        }

        private static void StartProcess()
        {
            if (setup[25] == "Y")
            {
                //Delete Old DB 
                LocalDBconn = new SqlConnection(LocalDB);
                int BackDays = Convert.ToInt32(setup[26]);
                BackDays = BackDays * -1;
                LocalDBconn.Open();
                from = DateTime.Today.AddDays(BackDays).ToString("M/d/yyyy HH:mm:ss");
                string FlowDel = "DELETE FROM dataExportEwp WHERE datetime <= '" + from + "'";
                string PresDel = "DELETE FROM datapoints WHERE DataTime <='" + from + "'";
                Console.WriteLine("Deleting DB Data before to Date : " + from);
                Console.WriteLine("--------------------------------------------------");
                SqlCommand DelFlw = new SqlCommand(FlowDel, LocalDBconn);
                DelFlw.ExecuteNonQuery();

                SqlCommand DelPres = new SqlCommand(PresDel, LocalDBconn);
                DelPres.ExecuteNonQuery();

                if (File.Exists(logpath))
                {
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Deleting DB Data before to Date :" + from);
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }
                else
                {
                    //File.CreateText(logpath);
                    File.WriteAllText(logpath, "");
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Deleting DB Data before to Date : " + from);
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }
                LocalDBconn.Close();
            }
            else
            {
                if (File.Exists(logpath))
                {
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Deleting DB Data : DISABLED");
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }
                else
                {
                    //File.CreateText(logpath);
                    File.WriteAllText(logpath, "");
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Deleting DB Data : DISABLED");
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }
            }

            if (setup[9] == "A")
            {
                typeprocess = "A";
                int dt = Convert.ToInt32(setup[21]);
                dt = dt * -1;
                //From_Date = DateTime.Today.AddDays(-91);
                //To_Date = DateTime.Today.AddDays(-90);

                if (setup[23].ToString() == "24")
                {
                    int correctHr = Convert.ToInt32(setup[24]);
                    correctHr = correctHr * -1;
                    From_Date = DateTime.Now.AddDays(dt).AddHours(correctHr);
                    From_Date = From_Date.AddMinutes(-From_Date.Minute);
                    From_Date = From_Date.AddSeconds(-From_Date.Second);

                    To_Date = DateTime.Now.AddDays(dt + 1).AddHours(correctHr);
                    //To_Date = DateTime.Now.AddHours(correctHr);
                    To_Date = To_Date.AddMinutes(-To_Date.Minute);
                    To_Date = To_Date.AddSeconds(-To_Date.Second);
                }
                else if (setup[23].ToString() == "DAY")
                {
                    From_Date = DateTime.Today.AddDays(dt);
                    //To_Date = DateTime.Now.AddDays(dt + 1);
                    To_Date = DateTime.Today;
                }

                Console.WriteLine("Trigger Auto from " + From_Date + " To " + To_Date);
                Console.WriteLine("--------------------------------------------------");

                if (File.Exists(logpath))
                {
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Trigger Auto from " + From_Date + " To " + To_Date);
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }
                else
                {
                    File.WriteAllText(logpath, "");
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Trigger Auto from " + From_Date + " To " + To_Date);
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }

            }
            else if (setup[9] == "M")
            {
                typeprocess = "M";
            }
            Object FromDate = From_Date;
            Object ToDate = To_Date;
            Object Type = typeprocess;

            starttoconvert(From_Date, To_Date, typeprocess);
        }



        private static void starttoconvert(DateTime from_Date, DateTime to_Date, string typeprocess)
        {
            Int32 Hours = Convert.ToInt32(setup[14]);
            Int32 Mins = Convert.ToInt32(setup[15]);
            Int32 FChannel = Convert.ToInt32(setup[16]);
            Int32 PChannel = Convert.ToInt32(setup[17]);
            Int32 InsertType = Convert.ToInt32(setup[20]);

            try
            {

                string query = "SELECT MIN(MeterRead) AS MINIMUM, MAX(MeterRead) AS MAXIMUM, SUM(Consumption) FROM dataExportEwp";
                string query_flow = "SELECT MAX (datapoints.value) FROM datapoints INNER JOIN sites ON datapoints.SiteID = sites.ID WHERE(datapoints.ChannelNumber = " + FChannel + ")";
                string disQuery = "SELECT sites.SiteID FROM sites INNER JOIN loggers ON sites.LoggerID = loggers.ID";
                //string GetSNo = "SELECT SNo FROM Sites_SNo";
                string LoggerDetailsQuery = "SELECT loggers.LoggerSerialNumber, loggers.LoggerSMSNumber, sites.SiteID, sites.LatEast, sites.LongNorth FROM loggers INNER JOIN sites ON loggers.ID = sites.LoggerID";
                //string pressurequery = "SELECT AVG (datapoints.value) FROM sites INNER JOIN loggers ON sites.LoggerID = loggers.ID INNER JOIN datapoints ON sites.LoggerID = datapoints.SiteID";
                string pressurequery = "SELECT AVG(datapoints.value) FROM datapoints INNER JOIN sites ON datapoints.SiteID = sites.LoggerID ";
                string ActSiteCount = "Select COUNT(*) FROM dataExportEwp";

                LocalDBconn = new SqlConnection(LocalDB);
                //bwssbcon = new SqlConnection(connectionstring);

                //NGPDBconn = new SqlConnection(RemoteDB);
                //NGPDBconn.Open();

                LocalDBconn.Open();

                //disQuery = "SELECT DISTINCT Mprn FROM dataExportEwp WHERE datetime='" + to_Date.ToString("M/d/yyyy HH:mm:ss") + "' ORDER BY Mprn ASC";
                disQuery = "SELECT DISTINCT Mprn FROM dataExportEwp WHERE datetime='" + to_Date.ToString("yyyy/M/d HH:mm:ss") + "' ORDER BY Mprn ASC";
                SqlCommand QueryChk = new SqlCommand(disQuery, LocalDBconn);
                results = new List<LocationsID>();
                using (SqlDataReader oReader = QueryChk.ExecuteReader())
                {
                    while (oReader.Read())
                    {
                        LocationsID Loc = new LocationsID();
                        Loc.LocID = oReader[0].ToString();
                        results.Add(Loc);
                    }
                }

                //Get SNo from Remote Server
                Console.WriteLine("Total Sites: " + results.Count);
                if (File.Exists(logpath))
                {
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Total Sites : " + results.Count);
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                        tw.WriteLine("\n");
                    }
                }
                else
                {
                    File.WriteAllText(logpath, "");
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Total Sites Active : " + results.Count);
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                        tw.WriteLine("\n");
                    }
                }

                LocalDBconn.Close();
                List<DateTime> dates = new List<DateTime>();

                for (var dt = from_Date; dt < to_Date; dt = dt.AddHours(Hours).AddMinutes(Mins))
                {
                    dates.Add(dt);
                }

                //Get Logger Details
                int SC = 0;
                for (i = 0; i < results.Count; i++)
                {
                    Location = results[i].LocID.Substring(0, results[i].LocID.IndexOf('_'));
                    LocalDBconn.Open();
                    String query2_loggerDetails = LoggerDetailsQuery + " Where sites.SiteID = '" + Location + "' ";
                    SqlCommand LogDetails = new SqlCommand(query2_loggerDetails, LocalDBconn);
                    //Logger Details
                    using (SqlDataReader oReader4 = LogDetails.ExecuteReader())
                    {
                        LogSerialNo = "0";
                        if (oReader4.HasRows)
                        {
                            while (oReader4.Read())
                            {
                                SC++;

                                if (String.IsNullOrEmpty(oReader4[1].ToString()))
                                {
                                    LogSIMNo = "0";
                                }
                                else
                                {
                                    LogSIMNo = oReader4[1].ToString();
                                }

                                if (String.IsNullOrEmpty(oReader4[0].ToString()))
                                {
                                    LogSerialNo = "0";
                                }
                                else
                                {
                                    LogSerialNo = oReader4[0].ToString();
                                }

                                if (String.IsNullOrEmpty(oReader4[3].ToString()))
                                {
                                    Long = 0;
                                }
                                else
                                {
                                    Long = Convert.ToDecimal(oReader4[3].ToString());
                                }

                                if (String.IsNullOrEmpty(oReader4[4].ToString()))
                                {
                                    Lat = 0;
                                }
                                else
                                {
                                    Lat = Convert.ToDecimal(oReader4[4].ToString());
                                }

                                if (File.Exists(logpath))
                                {
                                    using (var tw = new StreamWriter(logpath, true))
                                    {
                                        tw.WriteLine(SC + "  Location: {0}, Logger SNo.: {1}, Sim No: {2}", Location, LogSerialNo, LogSIMNo);
                                    }
                                }
                                else
                                {
                                    File.WriteAllText(logpath, "");
                                    using (var tw = new StreamWriter(logpath, true))
                                    {
                                        tw.WriteLine(SC + "  Location: {0}, Logger SNo.: {1}, Sim No: {2}", Location, LogSerialNo, LogSIMNo);
                                    }
                                }

                            }
                        }
                        LocalDBconn.Close();
                    }

                    if (LogSerialNo != "0")
                    {
                        bool exists;
                        NGPDBconn = new SqlConnection(LocalNgpDB);
                        NGPDBconn.Open();

                        try
                        {
                            // ANSI SQL way.  Works in PostgreSQL, MSSQL, MySQL.  
                            var cmd = new SqlCommand("select case when exists((select * from information_schema.tables where table_name = '" + LogSerialNo + "')) then 1 else 0 end", NGPDBconn);

                            exists = (int)cmd.ExecuteScalar() == 1;
                        }
                        catch
                        {
                            try
                            {
                                // Other RDBMS.  Graceful degradation
                                exists = true;
                                var cmdOthers = new SqlCommand("select 1 from " + LogSerialNo + " where 1 = 0", NGPDBconn);
                                cmdOthers.ExecuteNonQuery();
                            }
                            catch
                            {
                                exists = false;
                            }
                        }

                        if (exists == false)
                        {
                            //using (SqlCommand command = new SqlCommand("CREATE TABLE [" + LogSerialNo + "] (seriel_number int NULL,site_name varchar(50) NULL,phone_number varchar(50) NULL,longitude varchar(50) NULL,latitude varchar(50) NULL,data_type int NULL,channel_index varchar(50) NULL,id int NULL,value varchar(50) NULL,Datetime datetime NULL,FlowRate varchar(50) NULL)", NGPDBconn))
                            using (SqlCommand command = new SqlCommand("CREATE TABLE [" + LogSerialNo + "] (seriel_number int NULL,site_name varchar(50) NULL,phone_number varchar(50) NULL,longitude varchar(50) NULL,latitude varchar(50) NULL,data_type int NULL,channel_index varchar(50) NULL,id int NULL,value float NULL,Datetime datetime NULL,FlowRate float NULL)", NGPDBconn))

                                command.ExecuteNonQuery();
                        }
                        NGPDBconn.Close();
    
                    }
                }


                if (File.Exists(logpath))
                {
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }
                else
                {
                    File.WriteAllText(logpath, "");
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("\n");
                        tw.WriteLine("--------------------------------------------------");
                    }
                }

                //Processing Part Starts
                //for (i = 0; i < 5; i++)
                SC = 0;
                for (i = 0; i < results.Count; i++)
                {
                    k = 0;
                    int co = 0;
                    for (int j = dates.Count - 2; j >= 0; j--)
                    {
                        try
                        {

                            LocalDBconn.Open();
                            from = dates[j].AddSeconds(1).ToString("M/d/yyyy HH:mm:ss");
                            to = dates[j + 1].ToString("M/d/yyyy HH:mm:ss");
                            string prev = dates[j].AddHours(-Hours).AddMinutes(-Mins).AddSeconds(1).ToString("M/d/yyyy HH:mm:ss");

                            //if(setup[20] == "2") { Location = results[i].LocID.Substring(0, results[i].LocID.IndexOf('_')); }
                            //else if(setup[20] == "1")
                            //Location = results[i].LocID;
                            Location = results[i].LocID.Substring(0, results[i].LocID.IndexOf('_'));

                            String query2_CurrentTotalizer = query + " Where Mprn = '" + results[i].LocID + "' AND datetime >='" + from + "' AND datetime<='" + to + "'";
                            //String query2_CurrentTotalizer = query + " Where Mprn = 'khrone_2' AND datetime >='" + from + "' AND datetime<='" + to + "'";
                            String query2_CurrentFlow = query_flow + " AND (datapoints.DataTime >= '" + from + "') AND (datapoints.DataTime <= '" + to + "') AND (sites.SiteID = '" + Location + "')";

                            string query2_Pressure = pressurequery + " where sites.SiteID = '" + Location + "' AND datapoints.ChannelNumber= " + PChannel + " AND datapoints.DataTime >='" + from + "' AND datapoints.DataTime<='" + to + "'";

                            string query2_Loggeravailable = ActSiteCount + " WHERE Mprn = '" + results[i].LocID + "' AND datetime='" + to + "'";

                            SqlCommand CurrentTot = new SqlCommand(query2_CurrentTotalizer, LocalDBconn);
                            //Console.WriteLine(query2);
                            SqlCommand QueryFlow = new SqlCommand(query2_CurrentFlow, LocalDBconn);

                            SqlCommand Pressure = new SqlCommand(query2_Pressure, LocalDBconn);

                            SqlCommand CalledSite = new SqlCommand(query2_Loggeravailable, LocalDBconn);
                            //Console.WriteLine(query2_flow);

                            using (SqlDataReader oReader6 = CalledSite.ExecuteReader())
                            {
                                while (oReader6.Read())
                                {
                                    if (oReader6[0].ToString() != "0")
                                    {
                                        co = co + 1; ;
                                        if (co == 1) { actsite = actsite + 1; }
                                    }
                                }
                            }

                            using (SqlDataReader oReader2 = CurrentTot.ExecuteReader())
                            {
                                while (oReader2.Read())
                                {
                                    TotalFlowUnit = 1;
                                    TotalFlowUnitMk = "1";
                                    if (setup[10] == "m3") { TotalFlowUnit = 1; TotalFlowUnitMk = "1"; }
                                    else if (setup[10] == "Lts") { TotalFlowUnit = 1000; TotalFlowUnitMk = "0"; }

                                    CurrentPeriod = 0;
                                    if (String.IsNullOrEmpty(oReader2[1].ToString()))
                                    {
                                        CurrentPeriod = 0;
                                    }
                                    else
                                    {
                                        CurrentPeriod = Convert.ToDecimal(oReader2[1].ToString());
                                        if (setup[11] != "1")
                                        {
                                            CurrentPeriod = CurrentPeriod * Convert.ToDecimal(setup[12]) * Convert.ToDecimal(TotalFlowUnit);
                                            CurrentPeriod = Math.Round(CurrentPeriod, 3);
                                        }
                                    }

                                    Consumption = 0;
                                    if (String.IsNullOrEmpty(oReader2[2].ToString()))
                                    {
                                        Consumption = 0;
                                    }
                                    else
                                    {
                                        Consumption = Convert.ToDecimal(oReader2[2].ToString());
                                        Consumption = Math.Round(Consumption, 5);
                                    }
                                }
                            }

                            //Pressure
                            using (SqlDataReader oReader5 = Pressure.ExecuteReader())
                            {
                                while (oReader5.Read())
                                {

                                    if (String.IsNullOrEmpty(oReader5[0].ToString()))
                                    {
                                        PressureValue = 0;
                                    }
                                    else
                                    {
                                        PressureValue = Convert.ToDecimal(oReader5[0].ToString());
                                        PressureValue = Math.Round(PressureValue, 3);
                                    }

                                }
                            }

                            //Flow Rate
                            using (SqlDataReader oReader3 = QueryFlow.ExecuteReader())
                            {
                                ActFlowRate = 0;
                                FlowRate = 0;
                                while (oReader3.Read())
                                {
                                    if (Consumption != 0)
                                    {
                                        decimal hrs = Hours * 60;
                                        hrs = hrs + Mins;
                                        if (setup[11] == "m3.h") { hrs = hrs / 60; }

                                        FlowRate = Consumption / hrs;
                                        FlowRate = FlowRate * Convert.ToDecimal(setup[13]);
                                    }
                                    ActFlowRate = Math.Round(FlowRate, 3);
                                }
                            }

                            String query2_loggerDetails = LoggerDetailsQuery + " Where sites.SiteID = '" + Location + "' ";
                            SqlCommand LogDetails = new SqlCommand(query2_loggerDetails, LocalDBconn);
                            //Logger Details
                            using (SqlDataReader oReader4 = LogDetails.ExecuteReader())
                            {
                                LogSerialNo = "0";
                                while (oReader4.Read())
                                {
                                    if (String.IsNullOrEmpty(oReader4[1].ToString()))
                                    {
                                        LogSIMNo = "0";
                                    }
                                    else
                                    {
                                        LogSIMNo = oReader4[1].ToString();
                                    }

                                    if (String.IsNullOrEmpty(oReader4[0].ToString()))
                                    {
                                        LogSerialNo = "0";
                                    }
                                    else
                                    {
                                        LogSerialNo = oReader4[0].ToString();
                                    }

                                    if (String.IsNullOrEmpty(oReader4[3].ToString()))
                                    {
                                        Long = 0;
                                    }
                                    else
                                    {
                                        Long = Convert.ToDecimal(oReader4[3].ToString());
                                    }

                                    if (String.IsNullOrEmpty(oReader4[4].ToString()))
                                    {
                                        Lat = 0;
                                    }
                                    else
                                    {
                                        Lat = Convert.ToDecimal(oReader4[4].ToString());
                                    }
                                }
                            }

                            if (k == 0 && LogSerialNo != "0")
                            {
                                SC++;
                                if (setup[22].ToString() == "Y")
                                {
                                    NGPDBconn.Open();
                                    string deletedata = "DELETE FROM [" + LogSerialNo + "]";
                                    SqlCommand del = new SqlCommand(deletedata, NGPDBconn);
                                    del.ExecuteNonQuery();
                                    NGPDBconn.Close();
                                    k = 1;
                                }
                            }

                            if (InsertType == 2 && LogSerialNo != "0")
                            {

                                Double idflow = (Convert.ToDouble(LogSerialNo) * 10) + 1;
                                Double idPress = (Convert.ToDouble(LogSerialNo) * 10) + 2;
                                NGPDBconn = new SqlConnection(LocalNgpDB);
                                NGPDBconn.Open();

                                //string insertFlow = "INSERT INTO [" + LogSerialNo + "](seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',1,'D1a'," + idflow + ",'" + CurrentPeriod + "','" + to + "','" + ActFlowRate + "')";
                                //string insertPressure = "INSERT INTO [" + LogSerialNo + "](seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',2,'A1'," + idPress + ",'" + PressureValue + "','" + to + "','')";

                                string insertFlow = "INSERT INTO [" + LogSerialNo + "](seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',1,'D1a'," + idflow + "," + CurrentPeriod + ",'" + to + "'," + ActFlowRate + ")";
                                string insertPressure = "INSERT INTO [" + LogSerialNo + "](seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',2,'A1'," + idPress + "," + PressureValue + ",'" + to + "','')";


                                SqlCommand InFlow = new SqlCommand(insertFlow, NGPDBconn);
                                InFlow.ExecuteNonQuery();
                                SqlCommand InPress = new SqlCommand(insertPressure, NGPDBconn);
                                InPress.ExecuteNonQuery();

                                NGPDBconn.Close();
                                Console.WriteLine(SC + "  Data : Location: {0}, Logger SNo.: {5}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod, LogSerialNo);

                                if (File.Exists(logpath))
                                {
                                    using (var tw = new StreamWriter(logpath, true))
                                    {
                                        tw.WriteLine(SC + "  Data : Location: {0}, Logger SNo.: {5}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod, LogSerialNo);
                                    }
                                }
                                else
                                {
                                    File.WriteAllText(logpath, "");
                                    using (var tw = new StreamWriter(logpath, true))
                                    {
                                        tw.WriteLine(SC + "  Data : Location: {0}, Logger SNo.: {5}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod, LogSerialNo);
                                    }
                                }
                                Console.WriteLine("--------------------------------------------------");
                            }
                            else if (InsertType == 1 && LogSerialNo != "0")
                            {
                                NGPDBconn = new SqlConnection(LocalNgpDB);

                                NGPDBconn.Open();

                                //[Site_Read].[dbo].[60000]
                                string chkRows = "SELECT count(*) FROM [" + setup[19] + "].[dbo].[" + LogSerialNo + "]";
                                SqlCommand cmd = new SqlCommand(chkRows, NGPDBconn);
                                int RecCount = (int)cmd.ExecuteScalar();
                                if (RecCount > 0)
                                {
                                    string updateFlow = "UPDATE [" + LogSerialNo + "] SET value =" + CurrentPeriod + " ,Datetime ='" + to + "', FlowRate = " + ActFlowRate + " WHERE channel_index = 'D1a' AND seriel_number = " + LogSerialNo + "";
                                    string updatePress = "UPDATE [" + LogSerialNo + "] SET value =" + PressureValue + " ,Datetime ='" + to + "', FlowRate = '0.00' WHERE channel_index = 'A1' AND seriel_number = " + LogSerialNo + "";
                                    //NGPDBconn.Open();
                                    Console.WriteLine(SC + "  Data : Location: {0}, Logger SNo.:{5} Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => UPDATE", Location, to, PressureValue, ActFlowRate, CurrentPeriod, LogSerialNo);


                                    if (File.Exists(logpath))
                                    {
                                        using (var tw = new StreamWriter(logpath, true))
                                        {
                                            tw.WriteLine(SC + "  Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => UPDATE", Location, to, PressureValue, ActFlowRate, CurrentPeriod);
                                        }
                                    }
                                    else
                                    {
                                        File.WriteAllText(logpath, "");
                                        using (var tw = new StreamWriter(logpath, true))
                                        {
                                            tw.WriteLine(SC + "  Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod);
                                        }
                                    }

                                    SqlCommand InFlow = new SqlCommand(updateFlow, NGPDBconn);
                                    InFlow.ExecuteNonQuery();
                                    SqlCommand InPress = new SqlCommand(updatePress, NGPDBconn);
                                    InPress.ExecuteNonQuery();
                                    Console.WriteLine("--------------------------------------------------");
                                    //NGPDBconn.Close();
                                }
                                else if (RecCount == 0)
                                {

                                    Double idflow = (Convert.ToDouble(LogSerialNo) * 10) + 1;
                                    Double idPress = (Convert.ToDouble(LogSerialNo) * 10) + 2;

                                    string insertFlow = "INSERT INTO [" + LogSerialNo + "](seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',1,'D1a'," + idflow + ",'" + CurrentPeriod + "','" + to + "','" + ActFlowRate + "')";
                                    string insertPressure = "INSERT INTO [" + LogSerialNo + "](seriel_number,site_name,phone_number,longitude,latitude,data_type,channel_index,id,value,DateTime,FlowRate) VALUES (" + LogSerialNo + ",'" + Location + "','" + LogSIMNo + "','" + Long + "','" + Lat + "',2,'A1'," + idPress + ",'" + PressureValue + "','" + to + "','')";

                                    //NGPDBconn = new SqlConnection(RemoteDB);
                                    //NGPDBconn.Open();

                                    SqlCommand InFlow = new SqlCommand(insertFlow, NGPDBconn);
                                    InFlow.ExecuteNonQuery();
                                    SqlCommand InPress = new SqlCommand(insertPressure, NGPDBconn);
                                    InPress.ExecuteNonQuery();

                                    Console.WriteLine(SC + "  Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod);

                                    if (File.Exists(logpath))
                                    {
                                        using (var tw = new StreamWriter(logpath, true))
                                        {
                                            tw.WriteLine(SC + "  Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod);
                                        }
                                    }
                                    else
                                    {
                                        File.WriteAllText(logpath, "");
                                        using (var tw = new StreamWriter(logpath, true))
                                        {
                                            tw.WriteLine(SC + "  Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod);
                                        }
                                    }
                                    Console.WriteLine("--------------------------------------------------");
                                }
                                NGPDBconn.Close();
                            }
                           LocalDBconn.Close();
                        }
                        catch (Exception Ex)
                        {
                            Console.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());

                            if (File.Exists(logpath))
                            {
                                using (var tw = new StreamWriter(logpath, true))
                                {
                                    tw.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());
                                }

                            }
                            else
                            {
                                File.WriteAllText(logpath, "");
                                using (var tw = new StreamWriter(logpath, true))
                                {
                                    tw.WriteLine(SC + "  Data : Location: {0}, Date Time: {1}, Pressure: {2}, Flow Rate: {3}, Totalizer: {4} => INSERT", Location, to, PressureValue, ActFlowRate, CurrentPeriod);
                                }
                            }
                            // Console.ReadLine();
                            LocalDBconn.Close();
                        }

                    }

                    LocalDBconn.Close();
                    if (File.Exists(logpath))
                    {
                        using (var tw = new StreamWriter(logpath, true))
                        {
                            tw.WriteLine("--------------------------------------------------");
                        }
                    }
                    else
                    {
                        File.WriteAllText(logpath, "");
                        using (var tw = new StreamWriter(logpath, true))
                        {
                            tw.WriteLine("------------------------Total Active sites : " + actsite + " ----------------------");
                        }
                    }
                }
            }
            catch (Exception Ex)
            {
                Console.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());

                Console.ReadLine();
                if (File.Exists(logpath))
                {
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());
                    }
                }
                else
                {
                    File.WriteAllText(logpath, "");
                    using (var tw = new StreamWriter(logpath, true))
                    {
                        tw.WriteLine("Exception Occurred :{0},{1}", Ex.Message, Ex.StackTrace.ToString());
                    }
                }
                LocalDBconn.Close();
            }
        }
    }
}



