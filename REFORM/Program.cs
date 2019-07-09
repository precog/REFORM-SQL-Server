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
using System.Transactions;

namespace REFORM
{
    internal sealed class ExtraColumnCsvDataReader : CsvDataReader
    {
        private readonly string columnName;
        private readonly string columnValue;
        private readonly CsvReader csv;
        public ExtraColumnCsvDataReader(CsvReader csv, string columnName, string columnValue) : base(csv)
        {
            this.columnName = columnName;
            this.columnValue = columnValue;
            this.csv = csv;
        }

        override public object GetValue(int i)
        {
            return i == csv.Context.HeaderRecord.Length ? (object)this.columnValue : base.GetValue(i);
        }

        override public int FieldCount
        {
            get
            {
                return csv.Context.Record.Length + 1;
            }
        }


        override public bool IsDBNull(int i)
        {
            return (i == this.csv.Context.HeaderRecord.Length) ? false : base.IsDBNull(i);
        }

    }

    internal sealed class InfiniteTimeoutWebClient : WebClient
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
    class ReformListingTable
    {
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
                    return typeof(DateTimeOffset);
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

        static void Transfer(WebClient client, SqlConnection connection, SqlBulkCopy bulkCopy, CultureInfo culture, string serverConnectionString, string serverDatabase, string serverSchema, string countBytes, string writeMode, string defaultConstraintName, string defaultConstraintType, string defaultConstraint, string reformTableLink, string reformResultsLink)
        {
            using (Stream reformTableStream = client.OpenRead(reformTableLink))
            using (StreamReader reformTableStreamReader = new StreamReader(reformTableStream, System.Text.Encoding.UTF8))
            {
                String encodedTable = reformTableStreamReader.ReadToEnd();
                Console.WriteLine(encodedTable);
                ReformTable table = JsonConvert.DeserializeObject<ReformTable>(encodedTable);
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
                Column createdAt = new Column(newTable, defaultConstraintName, SqlType(defaultConstraintType));
                createdAt.Nullable = true;
                newTable.Columns.Add(createdAt);
                if (writeMode == "replace") { oldTable?.DropIfExists(); }
                if (oldTable == null || writeMode == "replace") { newTable.Create(); }
                using (Stream stream = client.OpenRead(reformResultsLink))
                using (StreamReader streamReader = new StreamReader(stream, System.Text.Encoding.UTF8))
                using (var csv = new CsvReader(streamReader))
                {
                    csv.Configuration.TypeConverterOptionsCache.GetOptions<string>().NullValues.Add("");
                    csv.Configuration.Encoding = System.Text.Encoding.UTF8;
                    csv.Configuration.Delimiter = ",";
                    csv.Configuration.LineBreakInQuotedFieldIsBadData = false;
                    csv.Configuration.CultureInfo = culture;
                    //csv.Configuration.BadDataFound = null;
                    if (countBytes == "count")
                    {
                        csv.Configuration.CountBytes = true;
                    }
                    using (var dataReader = new ExtraColumnCsvDataReader(csv, defaultConstraintName, defaultConstraint))
                    {
                        {
                            bulkCopy.DestinationTableName = $"{serverDatabase}.{serverSchema}.[{SqlName(table.Name)}]";
                            bulkCopy.EnableStreaming = true;
                            bulkCopy.BulkCopyTimeout = 0;
                            bulkCopy.WriteToServer(dataReader);
                        }
                    }
                }
            }
        }

        static void Main(string[] args)
        {
            string serverConnectionString = args[0];
            string serverDatabase = args[1];
            string serverSchema = args[2];
            string countBytes = args[3];
            string writeMode = args[4];
            string defaultConstraintName = args[5];
            string defaultConstraintType = args[6];
            string defaultConstraint = args[7];
            string reformBaseLink = args[8];
            string reformTableId;
            try {
                reformTableId = args[9];
            }
            catch
            {
                reformTableId = null;
            }
            CultureInfo culture = new CultureInfo("en-us", false);
            culture.NumberFormat.NumberDecimalSeparator = ".";
            Thread.CurrentThread.CurrentCulture = culture;
            ServicePointManager.ServerCertificateValidationCallback = new RemoteCertificateValidationCallback(delegate { return true; });
            using (TransactionScope scope = new TransactionScope())
            using (WebClient client = new InfiniteTimeoutWebClient())
            using (SqlConnection connection = new System.Data.SqlClient.SqlConnection(serverConnectionString))
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                connection.Open();
                if (reformTableId != null && reformTableId != "")
                {
                    String reformTableLink = $"{reformBaseLink}/api/table/{reformTableId}";
                    String encodedToken = client.UploadString($"{reformTableLink}/access-token", "");
                    ReformToken token = JsonConvert.DeserializeObject<ReformToken>(encodedToken);
                    String reformResultsLink = $"{reformBaseLink}/api/result/{token.Secret}.csv";
                    Console.WriteLine(reformResultsLink);
                    Transfer(client, connection, bulkCopy, culture, serverConnectionString, serverDatabase, serverSchema, countBytes, writeMode, defaultConstraintName, defaultConstraintType, defaultConstraint, reformTableLink, reformResultsLink);
                }
                else
                {
                    using (Stream reformTablesStream = client.OpenRead($"{reformBaseLink}/api/tables"))
                    using (StreamReader reformTablesStreamReader = new StreamReader(reformTablesStream, System.Text.Encoding.UTF8))
                    {
                        String encodedTables = reformTablesStreamReader.ReadToEnd();
                        Dictionary<String, ReformListingTable> tables = JsonConvert.DeserializeObject<Dictionary<String, ReformListingTable>>(encodedTables);
                        foreach (KeyValuePair<String, ReformListingTable> table in tables)
                        {
                            if (!table.Value.Name.Contains("[Archived]"))
                            {
                                String encodedToken = client.UploadString($"{reformBaseLink}/api/table/{table.Key}/access-token", "");
                                ReformToken token = JsonConvert.DeserializeObject<ReformToken>(encodedToken);
                                String reformResultsLink = $"{reformBaseLink}/api/result/{token.Secret}.csv";
                                Console.WriteLine(reformResultsLink);
                                Transfer(client, connection, bulkCopy, culture, serverConnectionString, serverDatabase, serverSchema, countBytes, writeMode, defaultConstraintName, defaultConstraintType, defaultConstraint, $"{reformBaseLink}/api/table/{table.Key}", $"{reformBaseLink}/api/result/{token.Secret}.csv");
                            }
                        }
                    }
                }
                scope.Complete();
            }
        }
    }
}
