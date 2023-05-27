using ElasticScaleDemo.Helper;
using log4net;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleDemo.Dao
{
    internal class ShardMapManagerManagement
    {
        static ILog logger = LogManager.GetLogger(typeof(ShardMapManagerManagement));

        public static ShardMapManager CreateShardMapManagerIfDoesNotExists()
        {
            // Create shardMap Manager in the shard map manager database
            string shardMapManagerConnectionString = SqlUtils.GetConnectionString(Constants.serverName, Constants.ShardMapManagerDatabaseName);
            bool shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                shardMapManagerConnectionString,
            ShardMapManagerLoadPolicy.Lazy,
                out ShardMapManager shardMapManager);
            if (shardMapManagerExists)
            {
                logger.Info("shard map already exists");
            }
            else
            {
                logger.Info("shard map did not exists, so creating one now");
                shardMapManager = ShardMapManagerFactory.CreateSqlShardMapManager(shardMapManagerConnectionString);
            }

            return shardMapManager;
        }
    }
}
