using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.DynamoDBv2.DocumentModel;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;

// Add using statements to access AWS SDK for .NET services. 
// Both the Service and its Model namespace need to be added 
// in order to gain access to a service. For example, to access
// the EC2 service, add:
// using Amazon.EC2;
// using Amazon.EC2.Model;

namespace AwsDynamoDBApp
{
    class Program
    {
        public static void Main(string[] args)
        {
            AmazonDynamoDBConfig config = new AmazonDynamoDBConfig();
            config.RegionEndpoint = RegionEndpoint.USEast1;
            AmazonDynamoDBClient client = new AmazonDynamoDBClient("AKIAJEGMJTKGB3HQDZWQ", "WYt2ov9zaSdgs8QlYJ1173m0Ts5K4/KrKgXFGhdN", config);

            // create table Movies
            string tableName = "Movies";
            CreateTable(client, "Movies");
            bool isCreated = false;
            do
            {
                isCreated = true;
           
                var tableStatus = GetTableStatus(client, "Movies");
                if(!object.Equals(tableStatus, TableStatus.ACTIVE))
                {
                    Console.WriteLine("The table {0} still being created. Please wait 5 seconds!");
                    Thread.Sleep(TimeSpan.FromSeconds(5));
                    isCreated = false;
                }
            } while (!isCreated);

            StreamReader streamReader = null;
            JsonTextReader jsonTextReader = null;
            JArray movieArray = null;

            try
            {
                //string path = string.Format("{0}/Data/{1}", AppDomain.CurrentDomain.BaseDirectory, "movidedata.json");
                streamReader = new StreamReader("moviedata.json");
                jsonTextReader = new JsonTextReader(streamReader);
                movieArray = (JArray)JToken.ReadFrom(jsonTextReader);

                Console.WriteLine(movieArray.Count);
            }
            catch (Exception ex)
            {
                Console.WriteLine("\n Error: could not read from the 'moviedata.json' file, because: " + ex.Message);
                PauseForDebugWindow();
                return;
            }
            finally
            {
                if (jsonTextReader != null)
                    jsonTextReader.Close();
                if (streamReader != null)
                    streamReader.Close();
            }

            Table movieTable = GetTable(client, tableName);
            Console.Write("\n   Now writing {0:#,##0} movie records from moviedata.json (might take 15 minutes)...\n   ...completed: ", movieArray.Count);

            for (int i = 0,j = 49; i < movieArray.Count; i++)
            {
                try
                {
                    string itemJson = movieArray[i].ToString();
                    Document document = Document.FromJson(itemJson);
                    movieTable.PutItem(document);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("\nError: Could not write the movie record #{0:#,##0}, because {1}", i, ex.Message);
                    PauseForDebugWindow();
                    continue;
                }

                if(i>=j)
                {
                    j++;
                    Console.WriteLine("{0,5:#,##0}, ", j);
                    if(j%1000 == 0)
                        Console.WriteLine("\n                      ");
                    j += 49;
                }
             
            }

            Console.WriteLine("\n   Finished writing all movie records to DynamoDB!");
            PauseForDebugWindow();
        }

        private static Table GetTable(AmazonDynamoDBClient client, string tableName)
        {
            return Table.LoadTable(client, tableName);
        }

        public static TableStatus GetTableStatus(AmazonDynamoDBClient client, string tableName)
        {
            var table = client.DescribeTable(tableName).Table;
            return (table == null) ? null : table.TableStatus;
        }

        public static void CreateTable(AmazonDynamoDBClient client, string tableName)
        {
            List<string> currentTables = client.ListTables().TableNames;
            if (currentTables.Contains(tableName))
            {
                Console.WriteLine("The table {0} is already exists.", tableName);
                return;
            }


            CreateTableRequest createRequest = new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new AttributeDefinition
                    {
                        AttributeName="year",
                        AttributeType = ScalarAttributeType.N
                    },
                    new AttributeDefinition
                    {
                        AttributeName="title",
                        AttributeType = "S"
                    }
                },

                KeySchema = new List<KeySchemaElement>()
                {
                    new KeySchemaElement
                    {
                        AttributeName="year",
                        KeyType = KeyType.HASH
                    },
                    new KeySchemaElement
                    {
                        AttributeName="title",
                        KeyType = "RANGE"
                    }
                }
            };

            createRequest.ProvisionedThroughput = new ProvisionedThroughput(1, 1);

            CreateTableResponse createResponse = client.CreateTable(createRequest);
            Console.WriteLine("\n\n Created the {0} table successfully!\n    Status of the new table: '{1}'", tableName, createResponse.TableDescription.TableStatus);
        }

        public static void PauseForDebugWindow()
        {
            // Keep the console open if in Debug mode...
            Console.Write("\n\n ...Press any key to continue");
            Console.ReadKey();
            Console.WriteLine();
        }
    }
}