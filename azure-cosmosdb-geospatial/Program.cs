using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace azure_cosmosdb_geospatial
{
    class Program
    {
        private static string ConnectionString = ConfigurationManager.AppSettings["ConnectionString"];
        private static string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static string ContainerName = ConfigurationManager.AppSettings["ContainerName"];
        private const int ConcurrentWorkers = 100;
        private const int ConcurrentDocuments = 1;

        private static CosmosClient cosmosClient;

        static async Task Main(string[] args)
        {

            Stopwatch sw = new Stopwatch();
            sw.Start();

            try
            {
                Program p = new Program();
                await p.Go();
            }
            catch (CosmosException ce)
            {
                Exception baseException = ce.GetBaseException();
                Console.WriteLine($"{ce.StatusCode} error occurred: {ce}");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }

        }

        public async Task Go()
        {
            cosmosClient = new CosmosClient(ConnectionString, new CosmosClientOptions()
            {
                AllowBulkExecution = true,
                ConnectionMode = ConnectionMode.Direct,
                MaxRequestsPerTcpConnection = -1,
                MaxTcpConnectionsPerEndpoint = -1,
                ConsistencyLevel = ConsistencyLevel.Eventual,
                MaxRetryAttemptsOnRateLimitedRequests = 999,
                MaxRetryWaitTimeOnRateLimitedRequests = TimeSpan.FromHours(1),
            });

            while (true)
            {
                PrintPrompt();

                var c = Console.ReadKey(true);
                switch (c.Key)
                {
                    case ConsoleKey.D0:
                        await Setup_Resources();
                        break;
                    case ConsoleKey.D1:
                        await Proximity_Query();
                        break;
                    case ConsoleKey.D2:
                        await Polygon_Query();
                        break;
                    case ConsoleKey.D3:
                        await Validate_Query();
                        break;
                    case ConsoleKey.D4:
                        await ValidateDetailed_Query();
                        break;
                    case ConsoleKey.Escape:
                        Console.WriteLine("Exiting...");
                        return;
                    default:
                        Console.WriteLine("Select choice");
                        break;
                }
            }
        }

        private void PrintPrompt()
        {
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");
            Console.WriteLine("Press for demo scenario:\n");

            Console.WriteLine("0 - Scenario 0: Setup Cosmos resources and import data");
            Console.WriteLine("1 - Scenario 1: Perform a proximity query against spatial data");
            Console.WriteLine("2 - Scenario 2: Perform a query to check if a point lies within a Polygon");
            Console.WriteLine("3 - Scenario 3: Perform a query to check if a spatial object is valid");
            Console.WriteLine("4 - Scenario 4: Perform a query to validate a Polygon that is not closed");

            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");
            Console.WriteLine("Press space key to exit.\n");
        }

        static async Task Setup_Resources()
        {
            Console.BackgroundColor = ConsoleColor.Blue;
            Console.ForegroundColor = ConsoleColor.Yellow;

            Console.WriteLine("Running setup...\n");
            Console.ResetColor();

            Database spatialDataDatabase = await cosmosClient.CreateDatabaseIfNotExistsAsync(DatabaseName);
            Console.WriteLine($"\tCreated database {spatialDataDatabase.Id}");
            Console.WriteLine("\tCreation of a 10000 RU/s container with Spatial indexes...");

            List<SpatialType> collectionOfSpatialTypes = new List<SpatialType>(4);
            collectionOfSpatialTypes.Add(SpatialType.Point);
            collectionOfSpatialTypes.Add(SpatialType.LineString);
            collectionOfSpatialTypes.Add(SpatialType.Polygon);
            collectionOfSpatialTypes.Add(SpatialType.MultiPolygon);

            await spatialDataDatabase.DefineContainer(ContainerName, "/year")
                    .WithIndexingPolicy()
                        .WithIndexingMode(IndexingMode.Consistent)
                        .WithAutomaticIndexing(true)
                        .WithIncludedPaths()
                            .Path("/*")
                            .Attach()
                        .WithExcludedPaths()
                            .Path("/\"_etag\"/?")
                            .Attach()
                        .WithSpatialIndex()
                            .Path("/geolocation/*", collectionOfSpatialTypes.ToArray())
                            .Attach()
                    .Attach()
                .CreateIfNotExistsAsync(10000);

            Container spatialDataContainer = cosmosClient.GetContainer(DatabaseName, ContainerName);
            Console.WriteLine($"\tCreated container {spatialDataContainer.Id} with spatial indexing policy");

            // Import data into the container
            Console.BackgroundColor = ConsoleColor.Blue;
            Console.ForegroundColor = ConsoleColor.Yellow;

            Console.WriteLine("\nStarting data import: this will take a few seconds...\n");
            Console.ResetColor();

            string filePath = @ConfigurationManager.AppSettings["FilePath"];

            List<Spatial> geospatialData = new List<Spatial>();
            using (FileStream fs = File.OpenRead(filePath))
            {
                geospatialData = await JsonSerializer.DeserializeAsync<List<Spatial>>(fs);
            }
                        
            Console.WriteLine("\tWriting data into Cosmos containers...");

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            // Enable bulk execution mode for faster import
            cosmosClient.ClientOptions.AllowBulkExecution = true;

            List<Task> tasks = new List<Task>(geospatialData.Count);
            int itemsCreated = 0;
            foreach (Spatial spatialItem in geospatialData)
            {
                // Create item in Nasa container
                tasks.Add(
                    spatialDataContainer.UpsertItemAsync<Spatial>(spatialItem, new PartitionKey(spatialItem.year)) // container is partitioned by year
                    .ContinueWith((Task<ItemResponse<Spatial>> task) =>
                    {
                        if (!task.IsCompletedSuccessfully)
                        {
                            AggregateException innerExceptions = task.Exception.Flatten();
                            CosmosException cosmosException = innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) as CosmosException;
                        }
                    }));
                ;

            }

            await Task.WhenAll(tasks);
            itemsCreated += tasks.Count(task => task.IsCompletedSuccessfully);

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);

            Console.WriteLine($"\tImport of items into {spatialDataContainer.Id} container completed with total time: {elapsedTime}");
            Console.WriteLine($"\tExecuted process with {tasks.Count} worker threads");
            Console.WriteLine($"\tInserted {itemsCreated} items");

            // Scale down the container to the minimum RU/s. 
            var currentThroughput = await spatialDataContainer.ReadThroughputAsync(requestOptions: new RequestOptions());
            int minThroughput = (int)currentThroughput.MinThroughput;
            var throughputResponse = await spatialDataContainer.ReplaceThroughputAsync(minThroughput);
            Console.WriteLine($"\tScaled container down to minimum {throughputResponse.Resource.Throughput} RU/s\n");

            // Reset bulk execution mode to false for rest of demo
            cosmosClient.ClientOptions.AllowBulkExecution = false;
        }

        static async Task Proximity_Query()
        {
            //Run query against container Nasa
            var coordinates = "[118.99, 32.94667]";
            var sqlQueryText = "SELECT * FROM c " + 
                               "WHERE ST_DISTANCE(c.geolocation, {" +
                                            "'type': 'Point', " + 
                                            "'coordinates':" + coordinates + 
                               "}) < 300000";

            await RunQuery(sqlQueryText);
        }

        static async Task Polygon_Query()
        {
            //Run query against container Nasa
            var coordinates = "[[118.99, 32.94667], [32, -5], [32, -4.7], [31.8, -4.7], [118.99, 32.94667]]";
            var sqlQueryText = "SELECT * FROM c " +
                               "WHERE ST_WITHIN(c.geolocation, {" +
                                            "'type':'Polygon', " +
                                            "'coordinates': [" + coordinates + "]" +
                               "})";

            await RunQuery(sqlQueryText);
        }

        static async Task Validate_Query()
        {
            //Run query against container Nasa
            var coordinates = "[118.99, 32.94667]";
            var sqlQueryText = "SELECT ST_ISVALID({ " + 
                                            "'type': 'Point', " + 
                                            "'coordinates': " + coordinates +
                               "})";

            await RunQuery(sqlQueryText);
        }

        static async Task ValidateDetailed_Query()
        {
            //Run query against container Nasa
            var coordinates = "[[118.99, 32.94667], [32, -5], [32, -4.7], [31.8, -4.7], [117, 32.94667]]";
            var sqlQueryText = "SELECT ST_ISVALIDDETAILED({ " + 
                                            "'type': 'Polygon', " +
                                            "'coordinates': [" + coordinates + "]" + 
                               "})";

            await RunQuery(sqlQueryText);
        }

        //Helper method to run query
        static async Task RunQuery(string sqlQueryText, int maxItemCountPerPage = 100, int maxConcurrency = -1, bool useQueryOptions = false)
        {
            Console.BackgroundColor = ConsoleColor.Blue;

            Console.WriteLine($"Running query: \"{sqlQueryText}\" against container {ContainerName}\n");
            Console.WriteLine("");

            if (useQueryOptions)
            {
                Console.WriteLine($"Using MaxConcurrency: {maxConcurrency}");
                Console.WriteLine($"Using MaxItemCountPerPage: {maxItemCountPerPage}");
            }
            Console.ResetColor();

            double totalRequestCharge = 0;
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            // Run query against Cosmos DB
            var container = cosmosClient.GetDatabase(DatabaseName).GetContainer(ContainerName);

            QueryRequestOptions requestOptions;
            if (useQueryOptions)
            {
                requestOptions = new QueryRequestOptions()
                {
                    MaxItemCount = maxItemCountPerPage,
                    MaxConcurrency = maxConcurrency,
                };
            }
            else
            {
                requestOptions = new QueryRequestOptions(); //use all default query options
            }

            // Time the query
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            FeedIterator<dynamic> queryResultSetIterator = container.GetItemQueryIterator<dynamic>(queryDefinition, requestOptions: requestOptions);
            List<dynamic> reviews = new List<dynamic>();

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<dynamic> currentResultSet = await queryResultSetIterator.ReadNextAsync();
                totalRequestCharge += currentResultSet.RequestCharge;
                //Console.WriteLine("another page");
                foreach (var item in currentResultSet)
                {
                    reviews.Add(item);
                    Console.WriteLine(item);
                }
                if (useQueryOptions)
                {
                    Console.WriteLine($"Result count: {reviews.Count}");
                }

            }

            stopWatch.Stop();
            TimeSpan ts = stopWatch.Elapsed;

            //Print results
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);

            Console.ForegroundColor = ConsoleColor.Green;

            Console.WriteLine($"\tQuery returned {reviews.Count} results");
            Console.WriteLine($"\tTotal time: {elapsedTime}");
            Console.WriteLine($"\tTotal Request Units consumed: {totalRequestCharge}\n");
            Console.WriteLine("\n\n\n");
            Console.ResetColor();

        }
    }
}