using System;
using System.Collections.Generic;
using System.IO;
using System.Data.SqlClient;
using System.ComponentModel;
using System.Data;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;
using System.Net.Http;
using System.Net;
using System.Net.Security;
using System.Linq;

namespace NOAAParser
{
    class Program
    {
        static String selectedTable;
        static bool waveData = true;
        static String[] existingTables = { "44018", "46050", "44065", "44017", "41117", "46219", "44009", "46028", "46219", "46014", "41025"   };
        static private SqlConnection Connect()
        {
            String connectString = "Server=LAPTOP-DTUMN89D;Database=buoys;Trusted_Connection=True;";
            SqlConnection con = new SqlConnection(connectString);
            return con;
        }

        static private void CreateTable()
        {
            String createQuery;
            String uploadQuery;
            SqlConnection conn = Connect();
            conn.Open();

            if (waveData == true)
            {
                createQuery = @"CREATE TABLE dbo.Buoy_" + selectedTable + @"
                (
                    IndexNum        INT    NOT NULL   PRIMARY KEY,
                    DateIndex datetime,
                    Year      [NVARCHAR](50)  NOT NULL,
                    Month  [NVARCHAR](50)  NOT NULL,
                    Day     [NVARCHAR](50)  NOT NULL,
                    Hour [NVARCHAR](50) NOT NULL,
                    Minute [NVARCHAR](50) NOT NULL,
                    WVHT [NVARCHAR](50) NOT NULL,
                    SwH [NVARCHAR](50) NOT NULL,
                    SwP [NVARCHAR](50) NOT NULL,
                    SwD [NVARCHAR](50) NOT NULL,
                    WWH [NVARCHAR](50) NOT NULL,
                    WWP [NVARCHAR](50) NOT NULL,
                    WWD [NVARCHAR](50) NOT NULL,
                    STEEPNESS [NVARCHAR](50) NOT NULL,
                    APD [NVARCHAR](50) NOT NULL,
                    MWD [NVARCHAR](50) NOT NULL

                );";
            }
            else
            {
                createQuery = @"CREATE TABLE dbo.Met_" + selectedTable + @"
                (
                    IndexNum        INT    NOT NULL   PRIMARY KEY,
                    DateIndex datetime,
                    Year      [NVARCHAR](50)  NOT NULL,
                    Month  [NVARCHAR](50)  NOT NULL,
                    Day     [NVARCHAR](50)  NOT NULL,
                    Hour [NVARCHAR](50) NOT NULL,
                    Minute [NVARCHAR](50) NOT NULL,
                    WDIR [NVARCHAR](50) NOT NULL,
                    WSPD [NVARCHAR](50) NOT NULL,
                    GST [NVARCHAR](50) NOT NULL,
                    WVHT [NVARCHAR](50) NOT NULL,
                    DPD [NVARCHAR](50) NOT NULL,
                    APD [NVARCHAR](50) NOT NULL,
                    MWD [NVARCHAR](50) NOT NULL,
                    PRES [NVARCHAR](50) NOT NULL,
                    ATMP [NVARCHAR](50) NOT NULL,
                    WTMP [NVARCHAR](50) NOT NULL,
                    DEWP [NVARCHAR](50) NOT NULL,
                    VIS [NVARCHAR](50) NOT NULL,
                    PTDY [NVARCHAR](50) NOT NULL,
                    TIDE [NVARCHAR](50) NOT NULL

                );";
            }
            
            SqlCommand createCmd = new SqlCommand(createQuery, conn);
            createCmd.ExecuteNonQuery();
            if (waveData == true)
            {
                uploadQuery = @"BULK
                INSERT dbo.Buoy_" + selectedTable + @" 
                FROM 'D:\Users\Shane\Documents\noaa\wave\" + selectedTable + @".csv'
                WITH
                (
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '0x0a',
                    FIRSTROW = 1
                )";
            }
            else
            {
                uploadQuery = @"BULK
                INSERT dbo.Met_" + selectedTable + @" 
                FROM 'D:\Users\Shane\Documents\noaa\met\" + selectedTable + @".csv'
                WITH
                (
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '0x0a',
                    FIRSTROW = 1
                )";
            }
            SqlCommand uploadCSV = new SqlCommand(uploadQuery, conn);
            uploadCSV.ExecuteNonQuery();

            conn.Close();
        }

        static private void DownloadTable()
        {
            String remoteAddress;
            String localAddress;
            WebClient client = new WebClient();
            if (waveData == true)
            {
                remoteAddress = "https://www.ndbc.noaa.gov/data/realtime2/" + selectedTable + ".spec";
                localAddress = "D:\\Users\\Shane\\Documents\\noaa\\wave\\" + selectedTable + ".spec";
            }
            else
            {
                remoteAddress = "https://www.ndbc.noaa.gov/data/realtime2/" + selectedTable + ".txt";
                localAddress = "D:\\Users\\Shane\\Documents\\noaa\\met\\" + selectedTable + ".txt";
            }

            client.DownloadFile(remoteAddress, localAddress);
        }

        static private int GetInt()
        {
            int selection;
            selection = Convert.ToInt32(Console.ReadLine());
            Console.WriteLine(selection);
            if (selection > 0 && selection < 3)
                return selection;
            else
                return 0;
        }

        static private int GetLatestIndex()
        {
            SqlConnection conn = Connect();
            int latest = 0;
            string dateQuery;
            try
            {
                if (waveData == true)
                {
                    dateQuery = "SELECT IndexNum FROM dbo.Buoy_" + selectedTable;
                }
                else
                {
                    dateQuery = "SELECT IndexNum FROM dbo.Met_" + selectedTable;
                } 
                SqlCommand cmd = new SqlCommand(dateQuery, conn);
                conn.Open();
                SqlDataReader dReader = cmd.ExecuteReader();
                if (dReader.HasRows)
                {
                    dReader.Read();
                    latest = dReader.GetInt32(0);
                }
                conn.Close();
                dReader.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return 0;
            }
            return latest;
        }

        static void InsertTable()
        {
            SqlConnection conn = Connect();
            conn.Open();
            String uploadQuery;
            
            if (waveData == true)
            {
                uploadQuery = @"BULK
                INSERT dbo.Buoy_" + selectedTable + @" 
                FROM 'D:\Users\Shane\Documents\noaa\wave\" + selectedTable + @".csv'
                WITH
                (
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '0x0a',
                    FIRSTROW = 1
                )";
            }
            else
            {
                uploadQuery = @"BULK
                INSERT dbo.Met_" + selectedTable + @" 
                FROM 'D:\Users\Shane\Documents\noaa\met\" + selectedTable + @".csv'
                WITH
                (
                    FIELDTERMINATOR = ',',
                    ROWTERMINATOR = '0x0a',
                    FIRSTROW = 1
                )";
            }
            SqlCommand uploadCSV = new SqlCommand(uploadQuery, conn);
            uploadCSV.ExecuteNonQuery();
            conn.Close();
        }

        static private void NewTable()
        {
            //Console.WriteLine("Please Enter Path to NOAA Data Table: ");
            //string pathToTable = Console.ReadLine();
            String pathtoTable;
            if (waveData == true)
            {
                pathtoTable = "D:\\Users\\Shane\\Documents\\noaa\\wave\\" + selectedTable + ".spec";
            }
            else
            {
                pathtoTable = "D:\\Users\\Shane\\Documents\\noaa\\met\\" + selectedTable + ".txt";
            }
            Console.WriteLine("User Submitted " + pathtoTable + " beginning parse...");
            ParseCSV(pathtoTable, new DateTime(0), 0);
        }
        static void ParseCSV(string path, DateTime latest, int latestIndex)
        {
            bool newTable = false;
            if (latestIndex == 0)
                newTable = true;
            string localPath;
            if (waveData == true)
            {
                localPath = "D:\\Users\\Shane\\Documents\\noaa\\wave\\" + selectedTable + ".csv";
            }
            else
            {
                localPath = "D:\\Users\\Shane\\Documents\\noaa\\met\\" + selectedTable + ".csv";
            }
            string latestString = latest.ToString("yyyy-MM-dd HH:mm:ss");
            int currentEntry = latestIndex;
            using (FileStream stream = File.Open(path, FileMode.Open))
            using (FileStream outputStream = File.Open(localPath, FileMode.Create))
            using (StreamWriter writer = new StreamWriter(outputStream))
            using (StreamReader reader = new StreamReader(stream))
            {
                string currentString;
                string[] stringArray;
                string parsedLine;
                string datetimeString;
                List<string> entryList = new List<string>();
                List<String> newEntries = new List<String>();
                bool final = false;

                while (!reader.EndOfStream && !final)
                {
                    currentString = reader.ReadLine();
                    stringArray = currentString.Split(" ");
                    parsedLine = "";
                    datetimeString = "";

                    for (int i = 0; i < stringArray.Length; i++)
                    {
                        if (!string.IsNullOrWhiteSpace(stringArray[i]))
                        {
                            entryList.Add(stringArray[i]);
                        }
                    }
                    for (int i = 0; i < entryList.Count; i++)
                    {
                        //Console.Write(entryList[i]);
                        parsedLine += entryList[i];
                        if (i <= 4)
                        {
                            datetimeString += entryList[i];
                            if (i == 0)
                                datetimeString += "-";
                            if (i == 1)
                                datetimeString += "-";
                            if (i == 2)
                                datetimeString += " ";
                            if (i == 3)
                                datetimeString += ":";
                            if (i == 4)
                                datetimeString += ":00";
                        }

                        if (i == entryList.Count - 1)
                        {
                            //Console.Write("\n");
                            parsedLine += "\n";
                            break;
                        }
                        //Console.Write(", ");
                        if (i == 4)
                        {
                            if (datetimeString.Contains(latestString))
                                final = true;
                            parsedLine = parsedLine.Insert(0, datetimeString + ", ");
                            parsedLine += ", ";
                        }
                        else
                        { parsedLine += ", "; }
                    }
                    //writer.Write(parsedLine);
                    //Console.Write(parsedLine);
                    //Console.WriteLine(datetimeString + " " + latestString);
                    if (!final)
                        newEntries.Add(parsedLine);
                    entryList.Clear();
                }
                for (int i = newEntries.Count - 1; i > 1; i--)
                {
                    parsedLine = "";
                    parsedLine = newEntries[i];
                    parsedLine = parsedLine.Insert(0, currentEntry + ", ");
                    writer.Write(parsedLine);
                    Console.Write(parsedLine);
                    currentEntry++;
                }
                writer.Close();
            }
        }

        static private int UpdateTable()
        {
            SqlConnection conn = Connect();
            DateTime latest = new DateTime(0);
            int latestIndex = 0;
            try
            {
                String dateQuery;
                if (waveData == true)
                {
                    dateQuery = "SELECT IndexNum, DateIndex FROM dbo.Buoy_" + selectedTable + " ORDER BY IndexNum DESC";
                }
                else
                {
                    dateQuery = "SELECT IndexNum, DateIndex FROM dbo.Met_" + selectedTable + " ORDER BY IndexNum DESC";
                }
                SqlCommand cmd = new SqlCommand(dateQuery, conn);
                conn.Open();
                SqlDataReader dReader = cmd.ExecuteReader();
                if (dReader.HasRows)
                {
                    dReader.Read();
                    latestIndex = dReader.GetInt32(0) + 1;
                    latest = dReader.GetDateTime(1);
                    Console.WriteLine(latest);
                }
                conn.Close();
                dReader.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                return -1;
            }

            string newData;

            if (waveData == true)
            {
                newData = "D:\\Users\\Shane\\Documents\\noaa\\wave\\" + selectedTable + ".spec";
            }
            else
            {
                newData = "D:\\Users\\Shane\\Documents\\noaa\\met\\" + selectedTable + ".txt";
            }
            ParseCSV(newData, latest, latestIndex);
            return 0;
        }
        static void Main(string[] args)
        {
            int selection = 0;
            Console.WriteLine("Select an Option: ");
            Console.WriteLine("1. Meteorological Data");
            Console.WriteLine("2. Wave Data");

            while (selection == 0)
            {
                selection = GetInt();
            }
            switch (selection)
            {
                case 1:
                    waveData = false;
                    break;
                case 2:
                    waveData = true;
                    break;
                default:
                    break;
            }

            selection = 0;
            Console.WriteLine("Select an option:");
            Console.WriteLine("1. Parse New Table");
            Console.WriteLine("2. Update Existing");
            while (selection == 0)
            {
                selection = GetInt();
            }
            switch (selection)
            {
                case 1:
                    Console.WriteLine("Enter Table ID Number: ");
                    selectedTable = Console.ReadLine();
                    if (existingTables.Contains(selectedTable))
                    {
                        Console.WriteLine("Table Already Exists. Exiting.");
                    }
                    else
                    {
                        DownloadTable();
                        NewTable();
                        CreateTable();
                        break;
                    }
                    break;
                case 2:
                    Console.WriteLine("Updating Existing Tables...");
                    for (int i = 0; i < existingTables.Length; i++)
                    {
                        selectedTable = "";
                        selectedTable = existingTables[i];
                        DownloadTable();
                        UpdateTable();
                        InsertTable();
                    }
                    break;
                default:
                    break;
            }
        }

    }
}
