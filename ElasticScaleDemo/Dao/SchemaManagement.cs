using ElasticScaleDemo.Helper;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace ElasticScaleDemo.Dao
{
    internal class SchemaManagement
    {
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
