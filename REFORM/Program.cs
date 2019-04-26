using CsvHelper;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Server;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer;
using System.Globalization;

namespace REFORM
{
    class ReformTable
    {
        [JsonProperty("columns")]
        public ReformColumn[] Columns { get; set; }
        [JsonProperty("name")]
        public String Name { get; set; }
    }
    class ReformColumn
    {
        [JsonProperty("column")]
        public String Name { get; set; }
        [JsonProperty("type")]
        public String Type { get; set; }
    }

    class Program
    {
        static DataType SqlType(String reformType)
        {
            switch (reformType)
            {
                case "offsetdatetime":
                    return DataType.DateTime;
                case "number":
                    return DataType.Money;
                case "string":
                    return DataType.Text;
                case "boolean":
                    return DataType.Bit;
            }
            return DataType.Text;
        }

        static Type CSType(String reformType)
        {
            switch (reformType)
            {
                case "offsetdatetime":
                    return typeof(DateTime);
                case "number":
                    return typeof(Decimal);
                case "string":
                    return typeof(String);
                case "boolean":
                    return typeof(Boolean);
            }
            return typeof(String);
        }

        static String SqlName(String reformName)
        {
            return $"{Regex.Replace(reformName, @"[^A-Za-z0-9]", "_")}";
        }
        static void Main(string[] args)
        {
            string serverConnectionString = args[0];
            string serverDatabase = args[1];
            string serverSchema = args[2];
            string reformAccessLink = args[3];
            using (WebClient client = new WebClient())
            {
                using (Stream reformTableStream = client.OpenRead(Regex.Replace(reformAccessLink, @"/live/dataset", "")))
                using (StreamReader reformTableStreamReader = new StreamReader(reformTableStream))
                {
                    String encodedTable = reformTableStreamReader.ReadToEnd();
                    ReformTable table = JsonConvert.DeserializeObject<ReformTable>(encodedTable);
                    System.Data.SqlClient.SqlConnection connection = new System.Data.SqlClient.SqlConnection(serverConnectionString);
                    Server server = new Server(new ServerConnection(connection));
                    Database database = server.Databases[serverDatabase];
                    Table newTable = new Table(database, SqlName(table.Name));
                    Table oldTable = database.Tables[SqlName(table.Name)];
                    foreach (ReformColumn column in table.Columns)
                    {
                        Column newColumn = new Column(newTable, SqlName(column.Name), SqlType(column.Type));
                        newColumn.Nullable = true;
                        newTable.Columns.Add(newColumn);
                    }
                    oldTable?.Drop();
                    newTable.Create();
                    connection.Close();

                    using (Stream stream = client.OpenRead(reformAccessLink))
                    using (StreamReader streamReader = new StreamReader(stream))
                    using (var csv = new CsvReader(streamReader))
                    using (var dataReader = new CsvDataReader(csv))
                    {
                        csv.Configuration.TypeConverterOptionsCache.GetOptions<string>().NullValues.Add("");
                        using (SqlBulkCopy bulkCopy = new SqlBulkCopy(serverConnectionString))
                        {
                            bulkCopy.DestinationTableName = $"{serverDatabase}.{serverSchema}.[{SqlName(table.Name)}]";
                            bulkCopy.EnableStreaming = true;
                            bulkCopy.BulkCopyTimeout = 0;
                            bulkCopy.WriteToServer(dataReader);
                            bulkCopy.Close();
                        }
                    }
                }
            }
        }
    }
}
