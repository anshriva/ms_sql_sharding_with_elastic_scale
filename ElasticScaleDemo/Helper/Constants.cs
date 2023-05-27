using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema;

namespace ElasticScaleDemo.Helper
{
    internal class Constants
    {
        public const string serverName = "ANUBHAV";

        public const string masterDbName = "master";

        public const string RangeShardMapManagerDatabaseName = "range_shard_map";

        public const string RangeShardMapName = "range_studentIdShardMap";

        public const string RangeShardNameFormat = "range_student_{0}";

        public const string ListShardMapManagerDatabaseName = "list_shard_map";

        public const string ListShardMapName = "list_studentIdShardMap";

        public const string ListShardNameFormat = "list_student_{0}";

        public const string TableName = "student";

        public const string ShardInitializationScriptPath = "Scripts//InitializeShard.sql";

        public static SchemaInfo GetSchemaInfo()
        {
            SchemaInfo schemaInfo = new SchemaInfo();
            schemaInfo.Add(new ShardedTableInfo(TableName, "Id"));
            return schemaInfo;
        }
    }
}
