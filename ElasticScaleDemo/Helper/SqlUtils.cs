using log4net;
using System.Data.SqlClient;
using System.Text;

namespace ElasticScaleDemo.Helper
{
    internal class SqlUtils
    {
        static ILog logger = LogManager.GetLogger(typeof(Program));

        public static bool DatabaseExists(string server, string db)
        {
            using SqlConnection conn = new SqlConnection(GetConnectionString(server, Constants.masterDbName));
            conn.Open();
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = "select count(*) from sys.databases where name = @dbname";
            cmd.Parameters.AddWithValue("@dbname", db);
            cmd.CommandTimeout = 60;
            int count = (int)cmd.ExecuteScalar();
            bool exists = count > 0;
            logger.Info($"the database {db} server {server}, isExists = {exists}");
            return exists;
        }

        public static void CreateDatabase(string server, string db)
        {
            using SqlConnection conn = new SqlConnection(GetConnectionString(server, Constants.masterDbName));
            conn.Open();
            SqlCommand cmd = conn.CreateCommand();
            cmd.CommandText = string.Format("CREATE DATABASE {0}", db);
            int returnValue = cmd.ExecuteNonQuery();
            logger.Info($"returned {returnValue} on create database query");
        }


        public static void ExecuteSqlScript(string server, string db, string schemaFile)
        {
            logger.Info($"Executing script {schemaFile}");
            using SqlConnection conn = new(GetConnectionString(server, db));
            conn.Open();
            SqlCommand cmd = conn.CreateCommand();
            // Read the commands from the sql script file
            IEnumerable<string> commands = ReadSqlScript(schemaFile);

            foreach (string command in commands)
            {
                cmd.CommandText = command;
                cmd.CommandTimeout = 60;
                cmd.ExecuteNonQuery();
            }
        }

        public static string GetConnectionString(string serverName, string database)
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder
            {
                DataSource = serverName,
                InitialCatalog = database,
                IntegratedSecurity = true,
                ApplicationName = "ESC_SKv1.0",
                ConnectTimeout = 30
            };
            return connStr.ToString();
        }

        public static string GetCredentialConnectionString()
        {
            SqlConnectionStringBuilder connStr = new SqlConnectionStringBuilder
            {
                IntegratedSecurity = true,
                ApplicationName = "ESC_SKv1.0",
                ConnectTimeout = 30
            };
            return connStr.ToString();
        }


        private static IEnumerable<string> ReadSqlScript(string scriptFile)
        {
            List<string> commands = new List<string>();
            using (TextReader tr = new StreamReader(scriptFile))
            {
                StringBuilder sb = new StringBuilder();
                string? line;
                while (( line = tr.ReadLine()) != null)
                {
                    if (line == "GO")
                    {
                        commands.Add(sb.ToString());
                        sb.Clear();
                    }
                    else
                    {
                        sb.AppendLine(line);
                    }
                }
            }

            return commands;
        }
    }
}
