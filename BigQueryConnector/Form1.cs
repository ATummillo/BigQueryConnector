using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;

namespace BigQueryConnector
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            //---------------- GLOBALS ----------------

            //Set GOOGLE_APPLICATION_CREDENTIALS env variable so that GoogleCredential.GetApplicationDefault() works
            Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", @"..\..\Credentials\key.json");
            string PROJECT_ID = "bqconnectordemo";

            //Create authenticated client
            var credential = GoogleCredential.GetApplicationDefault();
            var client = BigQueryClient.Create(PROJECT_ID, credential);

            //Set datasetId and tableName variables to identify the dataset and table to interact with (querying or streaming)
            string datasetId = "demo";
            string tableName = "BitcoinBlockchainMetrics";

            //----------------------------------------------
            //---------------- QUERY IMPORT ----------------

            //Query a table
            var table = client.GetTable(PROJECT_ID, datasetId, tableName);
            var sql =
                $"SELECT * FROM {table} WHERE DATE(DateTime) = '2018-09-17'";

            //Execute Query
            var results = client.ExecuteQuery(sql, parameters: null);

            //Create and populate DataTable
            DataTable dt = CreateDataTable(results);

            foreach (var row in results)
            {
                PopulateDataTable(dt, row);
            }

            //Populate DataGridView
            dataGridView1.DataSource = dt;
            dataGridView1.Refresh();

            //--------------------------------------------------
            //---------------- STREAMING EXPORT ----------------

            List<BigQueryInsertRow> bQIRows = new List<BigQueryInsertRow>();
            string[] columnNames = dt.Columns
                .Cast<DataColumn>()
                .Select(x => x.ColumnName)
                .ToArray();

            var count = 0; //DEMO ONLY!!!

            //Build list of rows to stream (bQIRows)
            foreach (DataRow dr in dt.Rows)
            {
                if (count > 10)
                {
                    break;
                }
                bQIRows.Add(PopulateBigQueryInsertRow(dr, columnNames, client, datasetId, tableName));
                count++; //DEMO ONLY!!!
            }

            //Stream bQIRows up to BigQuery
            client.InsertRows(datasetId, tableName, bQIRows);

            //---------------------------------------------------
            //---------------- MISC EXAMPLE CODE ----------------

            //Set query options
            //var queryOptions = new QueryOptions
            //{
            //    UseQueryCache = false
            //};
            //var results = client.ExecuteQuery(sql, parameters: null, queryOptions: queryOptions);

            //Console.WriteLine(results.SafeTotalRows);
            //var job = client.GetJob(results.JobReference);
            //var stats = job.Statistics;
            //Console.WriteLine("-----------");
            //Console.WriteLine($"Creation time: {stats.CreationTime}");
            //Console.WriteLine($"End time: {stats.EndTime}");
            //Console.WriteLine($"Total bytes processed: {stats.TotalBytesProcessed}");
        }


        private static void PopulateDataTable(DataTable dt, BigQueryRow row)
        {
            DataRow dr = dt.NewRow();

            foreach (var field in row.Schema.Fields)
            {
                dr[field.Name] = row[field.Name];
            }

            dt.Rows.Add(dr);
        }

        private static DataTable CreateDataTable(BigQueryResults res)
        {
            DataTable dt = new DataTable();

            foreach (var field in res.Schema.Fields)
            {
                dt.Columns.Add(field.Name);
            }

            return dt;
        }

        private static BigQueryInsertRow PopulateBigQueryInsertRow(DataRow dr, string[] cNames, BigQueryClient client, string datasetId, string tableName)
        {
            var bQIRow = new BigQueryInsertRow();
            string type;

            foreach (var key in cNames)
            {
                type = GetBigQueryType(client, datasetId, tableName, key);

                if (type == "TIMESTAMP")
                {
                    string value = DateTime.Parse((string)dr[key]).ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss");
                    bQIRow.Add(key, value);
                }
                else
                {
                    bQIRow.Add(key, dr[key]);
                }
            }

            return bQIRow;
        }

        private static string GetBigQueryType(BigQueryClient client, string datasetName, string tableName, string key)
        {
            BigQueryTable table = client.GetTable(datasetName, tableName);

            foreach (var field in table.Schema.Fields)
            {
                if (field.Name == key)
                {
                    return field.Type;
                }
            }
            return null;
        }
    }
}
