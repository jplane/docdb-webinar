
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;

namespace DocDBValidator
{
    public class Validator
    {
        private readonly ConnectionPolicy _connectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp,
            MaxConnectionLimit = 1000,
            EnableEndpointDiscovery = true,
            PreferredLocations = { }
        };

        private const int MinThreadPoolSize = 100;

        public Validator()
        {
            this.Database = "cdc-stats";
            this.Collection = "city-deaths";
        }

        public string EndpointUri { get; set; }

        public string AuthKey { get; set; }

        public string Database { get; set; }

        public string Collection { get; set; }

        public string[] PreferredLocations
        {
            get { return _connectionPolicy.PreferredLocations.ToArray(); }

            set
            {
                _connectionPolicy.PreferredLocations.Clear();

                foreach (var location in value)
                {
                    _connectionPolicy.PreferredLocations.Add(location);
                }
            }
        }

        public Tuple<int, string> Execute()
        {
            ThreadPool.SetMinThreads(MinThreadPoolSize, MinThreadPoolSize);

            using (var client = new DocumentClient(new Uri(this.EndpointUri), this.AuthKey, _connectionPolicy))
            {
                var collRef = UriFactory.CreateDocumentCollectionUri(this.Database, this.Collection);

                var sql = "SELECT VALUE 1 FROM C";

                var feedOptions = new FeedOptions
                {
                    EnableCrossPartitionQuery = true,
                    MaxDegreeOfParallelism = 100,
                    MaxBufferedItemCount = -1
                };

                var query = client.CreateDocumentQuery(collRef, sql, feedOptions);

                return Tuple.Create(query.ToArray().Length, client.ReadEndpoint.ToString());
            }
        }
    }
}
