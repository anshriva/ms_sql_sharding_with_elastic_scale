using ElasticScaleDemo.Helper;
using log4net;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    }
}
