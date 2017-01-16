
using System;
using System.Configuration;
using System.Threading;
using System.Threading.Tasks;

namespace DocDBValidator
{
    public sealed class Program
    {
        public static void Main(string[] args)
        {
            var endpoint = ConfigurationManager.AppSettings["EndPointUrl"];
            var authKey = ConfigurationManager.AppSettings["AuthorizationKey"];
            var databaseName = ConfigurationManager.AppSettings["DatabaseName"];
            var dataCollectionName = ConfigurationManager.AppSettings["CollectionName"];
            var preferredLocations = ConfigurationManager.AppSettings["PreferredLocations"].Split(',');

            var validator = new Validator
            {
                EndpointUri = endpoint,
                AuthKey = authKey,
                Database = databaseName,
                Collection = dataCollectionName,
                PreferredLocations = preferredLocations
            };

            var task = new Task(() =>
            {
                while (true)
                {
                    var result = validator.Execute();

                    Console.WriteLine($"There are {result.Item1} documents in the collection.");

                    Console.WriteLine($"Current read region is {result.Item2}");

                    Thread.Sleep(3000);
                }
            });

            task.Start();

            Console.WriteLine("[Enter] to quit");

            Console.ReadLine();
        }
    }
}
