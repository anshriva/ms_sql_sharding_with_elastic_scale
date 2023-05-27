using ElasticScaleDemo.Helper;
using ElasticScaleDemo.Dao;
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
            // DemoRangeShards();
            DemoListShards();
        }


        private static void DemoListShards()
        {
            DatabaseManagement.CreateDatabaseIfDoesNotExists(Constants.ListShardMapManagerDatabaseName);
            ShardMapManager shardMapManager = ShardMapManagerManagement.CreateShardMapManagerIfDoesNotExists(Constants.ListShardMapManagerDatabaseName);
            ListShardMap<int> listShardMap = ShardMapManagement.CreateOrGetListShardMap(shardMapManager, Constants.ListShardMapName);
            SchemaManagement.CreateSchemaInfo(shardMapManager, listShardMap.Name);
            string shard1 = ShardManagement.CreateShardDb(listShardMap, Constants.ListShardNameFormat,0);
            string shard2 = ShardManagement.CreateShardDb(listShardMap, Constants.ListShardNameFormat,1);
            
            ShardManagement.MapPointToShard(listShardMap, 1, shard1);
            ShardManagement.MapPointToShard(listShardMap, 2, shard2);


            Client.AddStudent(1, "Anubhav", listShardMap, SqlUtils.GetCredentialConnectionString());
            Client.AddStudent(2, "Abhinav", listShardMap, SqlUtils.GetCredentialConnectionString());

            Client.PrintStudents(0, 20, listShardMap, SqlUtils.GetCredentialConnectionString());

        }

        private static void DemoRangeShards()
        {
            DatabaseManagement.CreateDatabaseIfDoesNotExists(Constants.RangeShardMapManagerDatabaseName);
            ShardMapManager shardMapManager = ShardMapManagerManagement.CreateShardMapManagerIfDoesNotExists(Constants.RangeShardMapManagerDatabaseName);
            RangeShardMap<int> rangeShardMap = ShardMapManagement.CreateOrGetRangeShardMap(shardMapManager, Constants.RangeShardMapName);
            SchemaManagement.CreateSchemaInfo(shardMapManager, rangeShardMap.Name);

            string shard1 = ShardManagement.CreateShardDb(rangeShardMap, Constants.RangeShardNameFormat,0);
            string shard2 = ShardManagement.CreateShardDb(rangeShardMap, Constants.RangeShardNameFormat,1);

            ShardManagement.MapRangeToShard(rangeShardMap, new Range<int>(0, 10), shard1);
            ShardManagement.MapRangeToShard(rangeShardMap, new Range<int>(10, 20), shard2);

            Client.AddStudent(9, "Anubhav", rangeShardMap, SqlUtils.GetCredentialConnectionString());
            Client.AddStudent(19, "Anubhav", rangeShardMap, SqlUtils.GetCredentialConnectionString());

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