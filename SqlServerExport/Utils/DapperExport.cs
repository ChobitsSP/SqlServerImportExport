using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration;
using System.IO.Compression;
using Microsoft.Data.SqlClient;

namespace SqlServerExport
{
    public static class DapperExport
    {
        public static Encoding GetEncoding()
        {
            var value = ConfigUtils.GetSectionValue("Backup:Encoding");
            return string.IsNullOrEmpty(value) ? Encoding.UTF8 : Encoding.GetEncoding(value);
        }

        public static async Task BackupToCsv()
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new SqlConnection(constr);
            var folder = ConfigUtils.GetSectionValue("Backup:Folder");
            var saveName = ConfigUtils.GetSectionValue("Backup:FileName") + $"_{DateTime.Now.ToString("yyMMddHHmmss")}";

            var dirPath = Path.Combine(folder, saveName);
            if (!Directory.Exists(dirPath))
            {
                Directory.CreateDirectory(dirPath);
            }

            var tableNames = await GetTableNames(conn);

            foreach (var tableName in tableNames)
            {
                var hasData = await HasData(conn, tableName);
                if (!hasData) continue;
                LogService.Info($"Exporting {tableName} Start");
                await TableToCsv(conn, tableName, dirPath);
                LogService.Info($"Exporting {tableName} End");
            }

            string zipPath = Path.Combine(folder, saveName + ".zip");
            ZipFile.CreateFromDirectory(dirPath, zipPath);
            Directory.Delete(dirPath, true);
        }

        public static async Task<IEnumerable<string>> GetTableNames(IDbConnection conn)
        {
            const string sql = "select name from sysobjects where xtype='u' order by name";
            var list = await conn.QueryAsync<string>(sql);
            return list;
        }

        public static async Task TableToCsv(IDbConnection conn, string tableName, string dir)
        {
            var sql = $"SELECT * FROM {tableName}";
            using var reader = await conn.ExecuteReaderAsync(sql);
            var filePath = Path.Combine(dir, $"{tableName}.csv");
            await WriteToCsv(reader, filePath);
        }

        static IEnumerable<string> GetColumns(IDataReader reader)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                yield return reader.GetName(i);
            }
        }

        static IEnumerable<string> GetCells(IDataReader reader)
        {
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var value = reader.GetValue(i);

                if (value == null)
                {
                    yield return null;
                }
                else if (value.GetType() == typeof(DateTime))
                {
                    yield return ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss");
                }
                else
                {
                    yield return value.ToString();
                }
            }
        }

        static async Task<bool> HasData(IDbConnection conn, string tableName)
        {
            return true;
            try
            {
                var sql = $"SELECT 1 FROM {tableName} WHERE ROWNUM = 1";
                var result = await conn.QueryFirstOrDefaultAsync<int?>(sql);
                return result == 1;
            }
            catch (Exception ex)
            {
                LogService.Warn($"Error checking data for table {tableName}: {ex.Message}");
                return false;
            }
        }

        static async Task WriteToCsv(IDataReader reader, string filePath)
        {
            using var writer = new StreamWriter(filePath);

            var csv = new CsvWriter(writer, new CsvConfiguration(new System.Globalization.CultureInfo("zh-CN"))
            {
                Encoding = GetEncoding(),
            });

            foreach (var columnName in GetColumns(reader))
            {
                csv.WriteField(columnName);
            }

            await csv.NextRecordAsync();

            while (reader.Read())
            {
                foreach (var v in GetCells(reader))
                {
                    csv.WriteField(v);
                }
                await csv.NextRecordAsync();
            }

            writer.Close();
        }

        static void CheckCellValue(IDataReader reader, string tableName, Func<string, bool> isMatch)
        {
            var columns = GetColumns(reader).ToArray();

            var set = new HashSet<string>();

            while (reader.Read())
            {
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var key = string.Join(":", tableName, columns[i]);
                    if (set.Contains(key)) continue;

                    var value = reader.GetValue(i);

                    if (value != null && value.GetType() == typeof(string) && isMatch((string)value))
                    {
                        set.Add(key);
                        LogService.Info(key);
                    }
                }
            }
        }
    }
}
