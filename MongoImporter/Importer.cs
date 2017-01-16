
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Security.Authentication;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoImporter
{
    public class Importer
    {
        private static readonly string[] Fields =
        {
            "year",
            "week",
            "week-ending-date",
            "region",
            "state",
            "city",
            "pneumonia-flu-deaths",
            "all-deaths",
            "less-than-year-old-all-deaths",
            "one-to-twenty-four-years-old-all-deaths",
            "twenty-five-to-fourty-four-years-old-all-deaths",
            "forty-five-to-sixty-four-years-old-all-deaths",
            "sixty-five-plus-all-deaths"
        };

        private const int MinThreadPoolSize = 100;

        public Importer()
        {
            this.Database = "cdc-stats";
            this.Collection = "city-deaths";
            this.DocumentCount = 5000;
        }

        public string ConnectionString { get; set; }
        
        public string Database { get; set; }

        public string Collection { get; set; }

        public int DocumentCount { get; set; }


        public Task Execute()
        {
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);

            var settings = MongoClientSettings.FromUrl(new MongoUrl(this.ConnectionString));

            settings.SslSettings = new SslSettings() { EnabledSslProtocols = SslProtocols.Tls12 };

            var client = new MongoClient(settings);

            var currentCollectionThroughput = 2000; // hard-coded for now :-)

            var taskCount = currentCollectionThroughput / 1000;

            var batchSize = this.DocumentCount / taskCount;

            var tasks = new List<Task>();

            var uploadCount = 0;

            using (var reader = File.OpenText("data.json"))
            {
                var jsonDoc = JObject.ReadFrom(new JsonTextReader(reader));

                var docsToInsert = new List<JToken[]>();

                foreach (var data in jsonDoc["data"].Children())
                {
                    var array = data.AsJEnumerable().Skip(8).ToArray();

                    docsToInsert.Add(array);

                    uploadCount++;

                    if (docsToInsert.Count == batchSize)
                    {
                        tasks.Add(InsertDocument(client, docsToInsert.ToArray()));
                        docsToInsert.Clear();
                    }

                    if (uploadCount == this.DocumentCount)
                    {
                        break;
                    }
                }

                if (docsToInsert.Count > 0)
                {
                    tasks.Add(InsertDocument(client, docsToInsert.ToArray()));
                }

                return Task.WhenAll(tasks);
            }
        }

        private async Task InsertDocument(MongoClient client, IEnumerable<JToken[]> docs)
        {
            var db = client.GetDatabase(this.Database);

            var coll = db.GetCollection<BsonDocument>(this.Collection);

            foreach (var doc in docs)
            {
                var newDictionary = Fields
                    .Select((name, idx) => Tuple.Create(name, doc[idx].Value<string>()))
                    .ToDictionary(tuple => tuple.Item1, tuple => tuple.Item2);

                newDictionary["location"] = newDictionary["city"] + " " + newDictionary["state"];

                await coll.InsertOneAsync(newDictionary.ToBsonDocument());
            }
        }
    }
}
