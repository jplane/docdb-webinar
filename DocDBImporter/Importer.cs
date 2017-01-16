
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading;

namespace DocDBImporter
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

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            RequestTimeout = new TimeSpan(1, 0, 0),
            MaxConnectionLimit = 1000,
            RetryOptions = new RetryOptions
            {
                MaxRetryAttemptsOnThrottledRequests = 10,
                MaxRetryWaitTimeInSeconds = 60
            }
        };

        private const int MinThreadPoolSize = 100;

        public Importer()
        {
            this.Database = "cdc-stats";
            this.Collection = "city-deaths";
            this.DocumentCount = 5000;
        }

        public string EndpointUri { get; set; }

        public string AuthKey { get; set; }

        public string Database { get; set; }

        public string Collection { get; set; }

        public int DocumentCount { get; set; }

        public Task Execute()
        {
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);

            var client = new DocumentClient(new Uri(this.EndpointUri), this.AuthKey, ConnectionPolicy);

            var taskCount = 2;  // hard-coded... there are means to derive an optimal value dynamically...

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
                        tasks.Add(InsertDocuments(client, docsToInsert.ToArray()));
                        docsToInsert.Clear();
                    }

                    if (uploadCount == this.DocumentCount)
                    {
                        break;
                    }
                }

                if (docsToInsert.Count > 0)
                {
                    tasks.Add(InsertDocuments(client, docsToInsert.ToArray()));
                }

                return Task.WhenAll(tasks);
            }
        }

        private async Task InsertDocuments(DocumentClient client, IEnumerable<JToken[]> docs)
        {
            foreach (var doc in docs)
            {
                var newDictionary = Fields
                    .Select((name, idx) => Tuple.Create(name, doc[idx].Value<string>()))
                    .ToDictionary(tuple => tuple.Item1, tuple => tuple.Item2);

                newDictionary["location"] = newDictionary["city"] + " " + newDictionary["state"];

                try
                {
                    await client.CreateDocumentAsync(
                            UriFactory.CreateDocumentCollectionUri(this.Database, this.Collection),
                            newDictionary,
                            new RequestOptions { });
                }
                catch (Exception e)
                {
                    if (e is DocumentClientException)
                    {
                        DocumentClientException de = (DocumentClientException)e;

                        if (de.StatusCode != HttpStatusCode.Forbidden)
                        {
                            Trace.TraceError("Failed to write {0}. Exception was {1}", JsonConvert.SerializeObject(newDictionary), e);
                        }
                    }
                }
            }
        }
    }
}
