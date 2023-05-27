using ElasticScaleDemo.Helper;
using log4net;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleDemo.Dao
{
    public class ShardManagement
    {
        static ILog logger = LogManager.GetLogger(typeof(ShardManagement));

        public static Shard MapRangeToShard(RangeShardMap<int> rangeShardMap, Range<int> rangeForNewShard, string shardDbName)
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
            
            ShardLocation shardLocation = new ShardLocation(Constants.serverName, shardDbName);
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

        public static Shard MapPointToShard(ListShardMap<int> listShardMap, int point, string shardDbName)
        {
            if(listShardMap.TryGetMappingForKey(point, out PointMapping<int> pointMapping))
            {
                logger.Info("mapping already exists");
                return pointMapping.Shard;
            }

            logger.Info("mapping does not exists for the shard, hence creating one");

            ShardLocation shardLocation = new ShardLocation(Constants.serverName, shardDbName);
            if (listShardMap.TryGetShard(shardLocation, out Shard shard))
            {
                logger.Info("shard is already added to shardMap");
            }
            else
            {
                logger.Info("shard is not added to shard map, hence adding");
                shard = listShardMap.CreateShard(shardLocation);
            }

            PointMapping<int> pointMap = listShardMap.CreatePointMapping(point, shard);
            logger.Info($"Mapped point {pointMap.Value} to shard {shard.Location.Database}");
            return shard;
        }

        public static string CreateShardDb(ShardMap shardMap, string shardNameFormat, int count)
        {
            string ShardDatabaseName = string.Format(shardNameFormat, count);
            bool dbExists = DatabaseManagement.CreateDatabaseIfDoesNotExists(ShardDatabaseName);
            if (!dbExists)
            {
                SqlUtils.ExecuteSqlScript(Constants.serverName, ShardDatabaseName, Constants.ShardInitializationScriptPath);
            }
            return ShardDatabaseName;
        }
    }
}
