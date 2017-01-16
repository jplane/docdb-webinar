
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Authentication;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using System.Configuration;

namespace MongoImporter
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = ConfigurationManager.AppSettings["MongoConnectionString"];
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
                        ConnectionString = connectionString,
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
