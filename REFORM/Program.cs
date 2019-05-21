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
using System.Threading;
using System.Net.Security;

namespace REFORM
{
    class InfiniteTimeoutWebClient : WebClient
    {
        protected override WebRequest GetWebRequest(Uri uri)
        {
            WebRequest lWebRequest = base.GetWebRequest(uri);
            lWebRequest.Timeout = Timeout.Infinite;
            ((HttpWebRequest)lWebRequest).ReadWriteTimeout = Timeout.Infinite;
            return lWebRequest;
        }
    }
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

    class ReformToken
    {
        [JsonProperty("secret")]
        public String Secret { get; set; }
    }

    class Program
    {
        public static string CurrentCulture { get; private set; }

        static DataType SqlType(String reformType)
        {
            switch (reformType)
            {
                case "offsetdatetime":
                    return DataType.DateTime;
                case "number":
                    return DataType.Money;
                case "string":
                    return DataType.NText;
                case "boolean":
                    return DataType.Bit;
            }
            return DataType.NText;
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
            string countBytes = args[3];
            string writeToExisting = args[4];
            string reformAccessLink = args[5];
            CultureInfo culture = new CultureInfo("en-us", false);
            culture.NumberFormat.NumberDecimalSeparator = ".";
            Thread.CurrentThread.CurrentCulture = culture;
            ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(delegate { return true; } );

            using (WebClient client = new InfiniteTimeoutWebClient())
            {

                using (Stream reformTableStream = client.OpenRead(Regex.Replace(reformAccessLink, @"/live/dataset", "")))
                using (StreamReader reformTableStreamReader = new StreamReader(reformTableStream, System.Text.Encoding.UTF8))
                {
                    String encodedTable = reformTableStreamReader.ReadToEnd();
                    Console.WriteLine(encodedTable);
                    ReformTable table = JsonConvert.DeserializeObject<ReformTable>(encodedTable);
                    if (writeToExisting != "true")
                    {
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
                        newTable.Create();
                        connection.Close();
                    }
                    using (Stream stream = client.OpenRead(reformAccessLink))
                    using (StreamReader streamReader = new StreamReader(stream, System.Text.Encoding.UTF8))
                    using (var csv = new CsvReader(streamReader))
                    {
                        csv.Configuration.TypeConverterOptionsCache.GetOptions<string>().NullValues.Add("");
                        csv.Configuration.Encoding = System.Text.Encoding.UTF8;
                        csv.Configuration.Delimiter = ",";
                        csv.Configuration.LineBreakInQuotedFieldIsBadData = false;
                        csv.Configuration.CultureInfo = culture;
                        //csv.Configuration.BadDataFound = null;
                        if (countBytes == "true")
                        {
                            csv.Configuration.CountBytes = true;
                        }
                        using (var dataReader = new CsvDataReader(csv))
                        {
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
}
