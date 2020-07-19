using System;
using System.Collections.Generic;
using System.IO;

namespace NOAAParser
{
    class Program
    {
        static void OpenFile(string path)
        {
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
                int currentEntry = 0;
                while (!reader.EndOfStream)
                {
                    currentString = reader.ReadLine();
                    stringArray = currentString.Split(" ");
                    parsedLine = "";
                    datetimeString = "";

                    for(int i = 0; i < stringArray.Length; i++)
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
                            if (i == 4 )
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
                            parsedLine = parsedLine.Insert(0, currentEntry + "," + datetimeString + ", ");
                            parsedLine += ", ";
                        }
                        else
                        { parsedLine += ", ";}
                    }
                    if (currentEntry > 1)
                        writer.Write(parsedLine);
                    currentEntry++;
                    Console.Write(parsedLine);
                    entryList.Clear();
                }
                writer.Close();
            }
        }
        static void Main(string[] args)
        {
            Console.WriteLine("Please Enter Path to NOAA Data Table: ");
            //string pathToTable = Console.ReadLine();
            string pathToTable = "C:\\Users\\memes\\OneDrive\\Desktop\\44065.spec.txt";
            Console.WriteLine("User Submitted " + pathToTable + " beginning parse...");
            OpenFile(pathToTable);
        }

    }
}
