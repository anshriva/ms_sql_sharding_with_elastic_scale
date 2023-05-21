using log4net;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticScaleDemo.Helper
{
    internal class ShardAdministration
    {
        static ILog logger = LogManager.GetLogger(typeof(ShardAdministration));

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

        public static ShardMapManager CreateShardMapManagerIfDoesNotExists()
        {
            // Create shardMap Manager in the shard map manager database
            string shardMapManagerConnectionString = SqlUtils.GetConnectionString(Constants.serverName, Constants.ShardMapManagerDatabaseName);
            ShardMapManager shardMapManager;
            bool shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                shardMapManagerConnectionString,
                ShardMapManagerLoadPolicy.Lazy,
                out shardMapManager);
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

        public static void CreateDatabaseIfDoesNotExists(string databaseName)
        {
            // Create shardMapManager database if it does not exisits
            bool dbExists = SqlUtils.DatabaseExists(Constants.serverName, databaseName);
            logger.Info($"db already exists = {databaseName}");
            if (!dbExists)
            {
                logger.Info($"creating the database name = {databaseName}");
                SqlUtils.CreateDatabase(Constants.serverName, databaseName);
            }
        }

        public static Shard CreateShard(RangeShardMap<int> rangeShardMap, Range<int> rangeForNewShard)
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
            CreateDatabaseIfDoesNotExists(ShardDatabaseName);
            SqlUtils.ExecuteSqlScript(Constants.serverName, ShardDatabaseName,  Constants.ShardInitializationScriptPath);
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

        public static void CreateSchemaInfo(ShardMapManager shardMapManager, string shardMapName)
        {
            bool found = false;
            var schemaInfoCollection = shardMapManager.GetSchemaInfoCollection();
            foreach (var schemaInfo in schemaInfoCollection)
            {
                if (schemaInfo.Key == shardMapName)
                {
                    found = true;
                    break;
                }
            }

            if (!found)
            {
                shardMapManager.GetSchemaInfoCollection().Add(shardMapName, Constants.GetSchemaInfo());
            }
        }
    }
}
