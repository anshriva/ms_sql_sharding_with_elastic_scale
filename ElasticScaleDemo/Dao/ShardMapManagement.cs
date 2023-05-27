using log4net;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleDemo.Dao
{
    internal class ShardMapManagement
    {
        static ILog logger = LogManager.GetLogger(typeof(ShardMapManagement));

        public static RangeShardMap<int> CreateOrGetRangeShardMap(ShardMapManager shardMapManager, string shardMapName)
        {
            RangeShardMap<int> shardMap;
            bool shardMapExists = shardMapManager.TryGetRangeShardMap(shardMapName, out shardMap);
            if (shardMapExists)
            {
                logger.Info($"shardmap already exisits {shardMapName}");
            }
            else
            {
                logger.Info("shard map does not exists, now creating one");
                shardMap = shardMapManager.CreateRangeShardMap<int>(shardMapName);
            }
            return shardMap;
        }

        public static ListShardMap<int> CreateOrGetListShardMap(ShardMapManager shardMapManager, string shardMapName)
        {
            ListShardMap<int> listShardMap;
            bool shardMapExists = shardMapManager.TryGetListShardMap(shardMapName, out listShardMap);
            if (shardMapExists)
            {
                logger.Info($"shardmap already exisits {shardMapName}");
            }
            else
            {
                logger.Info("shard map does not exists, now creating one");
                listShardMap = shardMapManager.CreateListShardMap<int>(shardMapName);
            }
            return listShardMap;
        }
    }
}
