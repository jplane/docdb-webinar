
using System;
using System.Configuration;

namespace DocDBImporter
{
    class Program
    {
        static void Main(string[] args)
        {
            var endpoint = ConfigurationManager.AppSettings["EndPointUrl"];
            var authKey = ConfigurationManager.AppSettings["AuthorizationKey"];
            var databaseName = ConfigurationManager.AppSettings["DatabaseName"];
            var dataCollectionName = ConfigurationManager.AppSettings["CollectionName"];

            var run = true;

            while (run)
            {
                Console.WriteLine("How many documents to upload?");

                int docsToUpload;
                var input = Console.ReadLine();

                if (input != null && int.TryParse(input, out docsToUpload))
                {
                    var importer = new Importer
                    {
                        EndpointUri = endpoint,
                        AuthKey = authKey,
                        Database = databaseName,
                        Collection = dataCollectionName,
                        DocumentCount = docsToUpload
                    };

                    importer.Execute().Wait();
                }
                else
                {
                    run = false;
                }
            }
        }
    }
}
