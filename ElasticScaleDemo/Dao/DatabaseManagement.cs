using ElasticScaleDemo.Helper;
using log4net;

namespace ElasticScaleDemo.Dao
{
    internal class DatabaseManagement
    {
        static ILog logger = LogManager.GetLogger(typeof(DatabaseManagement));

        public static bool CreateDatabaseIfDoesNotExists(string databaseName)
        {
            // Create shardMapManager database if it does not exisits
            bool dbExists = SqlUtils.DatabaseExists(Constants.serverName, databaseName);
            logger.Info($"db already exists = {databaseName}");
            if (!dbExists)
            {
                logger.Info($"creating the database name = {databaseName}");
                SqlUtils.CreateDatabase(Constants.serverName, databaseName);
            }
            return dbExists;
        }
    }
}
