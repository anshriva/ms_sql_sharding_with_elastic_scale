using log4net;
using Microsoft.Azure.SqlDatabase.ElasticScale.Query;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ElasticScaleDemo
{
    internal class Client
    {
        static ILog logger = LogManager.GetLogger(typeof(Client));

        public static int AddStudent(int id, String name, ShardMap shardMap, string connectionString)
        {
            using (SqlConnection conn = shardMap.OpenConnectionForKey(id, connectionString))
            {
                SqlCommand cmd = conn.CreateCommand();
                cmd.CommandText = $"insert into student(Id, Name)  VALUES (@Id, @Name)";
                cmd.Parameters.AddWithValue("@Id", id);
                cmd.Parameters.AddWithValue("@Name", name);
                int result = cmd.ExecuteNonQuery();
                logger.Info($"got the response from the db for adding the students = {result}");
                return result;
            }
        }

        public static void PrintStudents(int startId, int endId, RangeShardMap<int> shardMap, string connectionString)
        {
            using MultiShardConnection conn = new MultiShardConnection(shardMap.GetShards(), connectionString);
            using MultiShardCommand cmd = conn.CreateCommand();
            cmd.CommandText = $"select * from student where Id > @startId and Id < @endId";
            cmd.Parameters.AddWithValue("@startId", startId);
            cmd.Parameters.AddWithValue("@endId", endId);

            cmd.ExecutionOptions = MultiShardExecutionOptions.IncludeShardNameColumn;

            // Allow for partial results in case some shards do not respond in time
            cmd.ExecutionPolicy = MultiShardExecutionPolicy.PartialResults;

            // Allow the entire command to take up to 30 seconds
            cmd.CommandTimeout = 30;

            using MultiShardDataReader dataReader = cmd.ExecuteReader();
            logger.Info($"Below are all the students");

            while (dataReader.Read())
            {
                for (int i = 0; i < dataReader.FieldCount; i++)
                {
                    Console.Write(dataReader[i] + "\t");
                }
                Console.WriteLine();
            }
        }
    }
}
