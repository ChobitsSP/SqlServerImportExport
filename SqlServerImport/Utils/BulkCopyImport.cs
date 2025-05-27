using CsvHelper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlServerImport.Utils
{
    public static class BulkCopyImport
    {
        public static async Task ImportData(string csvFilePath, string tableName, int batchSize = 1000)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new SqlConnection(constr);

            await conn.OpenAsync();

            using var bulkCopy = new SqlBulkCopy(conn)
            {
                DestinationTableName = tableName,
                BatchSize = batchSize,
                BulkCopyTimeout = 60,
            };

            var table = ReadCsv(csvFilePath);
            await bulkCopy.WriteToServerAsync(table);

            await conn.CloseAsync();
        }

        public static Encoding GetEncoding()
        {
            var value = ConfigUtils.GetSectionValue("Backup:Encoding");
            return string.IsNullOrEmpty(value) ? Encoding.UTF8 : Encoding.GetEncoding(value);
        }

        public static DataTable ReadCsv(string filePath)
        {
            var dt = new DataTable();

            using var reader = new StreamReader(filePath, encoding: GetEncoding());
            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);

            // Do any configuration to `CsvReader` before creating CsvDataReader.
            using var dr = new CsvDataReader(csv);
            dt.Load(dr);

            return dt;
        }
    }
}
