using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement.Schema;

namespace ElasticScaleDemo.Helper
{
    internal class Constants
    {
        public const string serverName = "ANUBHAV";

        public const string ShardMapManagerDatabaseName = "shard_map";

        public const string masterDbName = "master";

        public const string ShardMapName = "StudentIdShardMap";

        public const string ShardNameFormat = "student_{0}";

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
