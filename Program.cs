using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;

namespace ES8.x
{
    internal class Program
    {
        const string IndexName = "stock-demo-v1";
        const string AliasName = "stock-demo";

        private static async Task Main(string[] args)
        {
            var settings = new ElasticsearchClientSettings(new Uri("https://localhost:9200"))
                               .CertificateFingerprint("8ace203afd337c98685de650429f11129844507793a7bd8c020b406c8d8cea66")
                               .Authentication(new BasicAuthentication("elastic", "qGRq0YVQbTFY6j7senwQ"));

            var Client = new ElasticsearchClient(settings);

            var existsResponse = await Client.Indices.ExistsAsync(IndexName);

            if (!existsResponse.Exists)
            {
                var newIndexResponse = await Client.Indices.CreateAsync(IndexName, i => i
                    .Mappings(m => m
                        .Properties(new Properties
                        {
                            { "symbol", new KeywordProperty()},
                            { "high", new FloatNumberProperty()},
                            { "low", new FloatNumberProperty()},
                            { "open", new FloatNumberProperty()},
                            { "close", new FloatNumberProperty()},
                        }))
                    .Settings(s => s.NumberOfShards(1).NumberOfReplicas(0)));
                if (!newIndexResponse.IsValidResponse || 
                    newIndexResponse.Acknowledged is false) throw new Exception("Oh no!!");

                var bulkAll = Client.BulkAll(ReadStockData(), r => r
                    .Index(IndexName)
                    .BackOffRetries(2)
                    .BackOffTime("30s")
                    .MaxDegreeOfParallelism(4)
                    .Size(1000));

                bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));

                var aliasResponse = await Client.Indices.PutAliasAsync(IndexName, AliasName);
                if (!aliasResponse.IsValidResponse) throw new Exception("Oh no!!");
            }
        }

        public static IEnumerable<StockData> ReadStockData()
        {
            // Update this to the correct path of the CSV file
            var file = new StreamReader("c:\\stock-data\\all_stocks_5yr.csv");

            string line;
            while ((line = file.ReadLine()) is not null) yield return new StockData(line);
        }
    }
}