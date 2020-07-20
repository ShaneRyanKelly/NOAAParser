using System;
using System.Collections.Generic;
using System.IO;
using System.Data.SqlClient;
using System.ComponentModel;
using System.Data;
using System.Runtime.CompilerServices;
using System.Runtime.ConstrainedExecution;

namespace NOAAParser
{
    class Program
    {
        static private SqlConnection Connect()
        {
            String connectString = "Server=LAPTOP-DTUMN89D;Database=buoys;Trusted_Connection=True;";
            SqlConnection con = new SqlConnection(connectString);
            return con;
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
            try
            {
                String dateQuery = "SELECT IndexNum FROM dbo.Buoy_44065";
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

        static private void NewTable()
        {
            Console.WriteLine("Please Enter Path to NOAA Data Table: ");
            //string pathToTable = Console.ReadLine();
            string pathToTable = "C:\\Users\\memes\\OneDrive\\Desktop\\44065.spec.txt";
            Console.WriteLine("User Submitted " + pathToTable + " beginning parse...");
            ParseCSV(pathToTable, new DateTime(0), 0);
        }
        static void ParseCSV(string path, DateTime latest, int latestIndex)
        {
            string latestString = latest.ToString("yyyy-MM-dd HH:mm:ss");
            int currentEntry = latestIndex;
            using (FileStream stream = File.Open(path, FileMode.Open))
            using (FileStream outputStream = File.Open("C:\\Users\\memes\\OneDrive\\Desktop\\44065.csv", FileMode.OpenOrCreate))
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
                String dateQuery = "SELECT IndexNum, DateIndex FROM dbo.Buoy_44065 ORDER BY IndexNum DESC";
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
            string newData = "C:\\Users\\memes\\OneDrive\\Desktop\\44065.spec.txt";
            ParseCSV(newData, latest, latestIndex);
            return 0;
        }
        static void Main(string[] args)
        {
            int selection = 0;
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
                    NewTable();
                    break;
                case 2:
                    UpdateTable();
                    break;
                default:
                    break;
            }
        }

    }
}
