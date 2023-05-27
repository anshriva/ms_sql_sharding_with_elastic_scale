using ElasticScaleDemo.Helper;
using log4net;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleDemo.Dao
{
    public class ShardManagement
    {
        static ILog logger = LogManager.GetLogger(typeof(ShardManagement));

        public static Shard CreateRangeShard(RangeShardMap<int> rangeShardMap, Range<int> rangeForNewShard)
        {
            int min = rangeForNewShard.Low;
            int max = rangeForNewShard.High;

            var mappings = rangeShardMap.GetMappings();
            foreach (var mapping in mappings)
            {
                int mapingLow = mapping.Value.Low;
                int mappingHigh = mapping.Value.High;
                logger.Info($"going to check if mapping exists for range [{min}, {max}] for [{mapingLow}, {mappingHigh}]");
                if (min < mapingLow && max < mappingHigh || min > mapingLow && max > mappingHigh)
                {
                    continue;
                }
                else
                {
                    logger.Info("range already exists");
                    return mapping.Shard;
                }
            }

            string ShardDatabaseName = string.Format(Constants.ShardNameFormat, rangeShardMap.GetShards().Count());
            DatabaseManagement.CreateDatabaseIfDoesNotExists(ShardDatabaseName);
            SqlUtils.ExecuteSqlScript(Constants.serverName, ShardDatabaseName, Constants.ShardInitializationScriptPath);
            ShardLocation shardLocation = new ShardLocation(Constants.serverName, ShardDatabaseName);
            if (rangeShardMap.TryGetShard(shardLocation, out Shard shard))
            {
                logger.Info("shard is already added to shardMap");
            }
            else
            {
                logger.Info("shard is not added to shard map, hence adding");
                shard = rangeShardMap.CreateShard(shardLocation);
            }

            RangeMapping<int> mappingForNewShard = rangeShardMap.CreateRangeMapping(rangeForNewShard, shard);
            logger.Info($"Mapped range {mappingForNewShard.Value} to shard {shard.Location.Database}");
            return shard;
        }

        public static Shard CreateListShard(ListShardMap<int> listShardMap)
        {
            // TODO : return the list shard
            return null;
        }
    }
}
