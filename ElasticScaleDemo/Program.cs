using ElasticScaleDemo.Helper;
using log4net;
using log4net.Config;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System.Reflection;

namespace ElasticScaleDemo
{
    internal class Program
    {
        static ILog logger = LogManager.GetLogger(typeof(Program));

        static void Main(string[] args)
        {
            InitializeLogger();
            ShardAdministration.CreateDatabaseIfDoesNotExists(Constants.ShardMapManagerDatabaseName);
            ShardMapManager shardMapManager = ShardAdministration.CreateShardMapManagerIfDoesNotExists();
            RangeShardMap<int> rangeShardMap = ShardAdministration.CreateOrGetRangeShardMap(shardMapManager, Constants.ShardMapName);
            ShardAdministration.CreateSchemaInfo(shardMapManager, rangeShardMap.Name);
            ShardAdministration.CreateShard(rangeShardMap, new Range<int>(0, 10));
            ShardAdministration.CreateShard(rangeShardMap, new Range<int>(10, 20));

            Client.AddStudent(20, "Anubhav", rangeShardMap, SqlUtils.GetCredentialConnectionString());
            Client.PrintStudents(0, 20, rangeShardMap, SqlUtils.GetCredentialConnectionString());

        }
      
        private static void InitializeLogger()
        {
            // Initialize logger
            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
            XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));
            logger = LogManager.GetLogger(typeof(Program));
        }
    }
}