using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CsvHelper;
using Dapper;
using Microsoft.Data.SqlClient;

namespace SqlServerImport.Utils
{
    public static class DapperImport
    {
        public static Encoding GetEncoding()
        {
            var value = ConfigUtils.GetSectionValue("Import:Encoding");
            return string.IsNullOrEmpty(value) ? Encoding.UTF8 : Encoding.GetEncoding(value);
        }

        public static string GetInsertSql(List<TableColumn> columns, string table_name)
        {
            var columnNames = string.Join(",", columns.Select(t => $"[{t.name}]"));
            var parameterNames = string.Join(",", columns.Select(t => "@" + t.name));
            return $"INSERT INTO {table_name} ({columnNames}) VALUES ({parameterNames})";
        }

        public class TableColumn
        {
            public int id { get; set; }
            public string name { get; set; }
            public string type { get; set; }
            public bool null_able { get; set; }
            public long? data_length { get; set; }
            public int? numeric_precision { get; set; }
            public int? numeric_scale { get; set; }
        }

        static async Task<List<TableColumn>> GetColumns(IDbConnection conn, string table_name)
        {
            var sql = @"
SELECT
  id = a.colorder,
  name = a.name,
  type = b.name,
  [table] = d.name,
  character_maximum_length = a.length,
  numeric_precision = a.prec,
  numeric_scale = a.scale,
  null_able = a.isnullable
FROM
  syscolumns a
  left join systypes b on a.xusertype = b.xusertype
  inner join sysobjects d on a.id = d.id
  and d.xtype = 'U'
  and d.name <> 'dtproperties'
  left join syscomments e on a.cdefault = e.id
  left join sys.extended_properties g on a.id = g.major_id
  and a.colid = g.minor_id
  left join sys.extended_properties f on d.id = f.major_id
  and f.minor_id = 0
where
  1 = 1
  and d.name = @table
order by
  d.name,
  a.id,
  a.colorder";

            var list = await conn.QueryAsync<TableColumn>(sql, new { table = table_name });
            return list.AsList();
        }

        public static IEnumerable<List<T>> GroupList<T>(IEnumerable<T> items, int count)
        {
            List<T> currentGroup = new List<T>();
            foreach (var item in items)
            {
                currentGroup.Add(item);
                if (currentGroup.Count == count)
                {
                    yield return currentGroup;
                    currentGroup = new List<T>();
                }
            }

            // 返回最后一组（可能包含少于 count 个元素）
            if (currentGroup.Count > 0)
            {
                yield return currentGroup;
            }
        }

        public static IEnumerable<Dictionary<string, string>> ReadCsv(string filePath)
        {
            using var reader = new StreamReader(filePath, encoding: GetEncoding());
            using var csvReader = new CsvReader(reader, CultureInfo.InvariantCulture);

            csvReader.Read();
            csvReader.ReadHeader();
            var headers = csvReader.HeaderRecord;

            while (csvReader.Read())
            {
                var record = new Dictionary<string, string>();
                foreach (var header in headers)
                {
                    var vala = csvReader[header];
                    record[header] = csvReader[header];
                }
                yield return record;
            }
        }

        static object GetInsertObj(Dictionary<string, string> row, List<TableColumn> columns)
        {
            var item = new Dictionary<string, object>();

            foreach (var key in row.Keys)
            {
                var column = columns.FirstOrDefault(t => string.Equals(t.name, key, StringComparison.OrdinalIgnoreCase));
                if (column == null) throw new Exception("column not found: " + key);
                var value = GetValueObj(row[key], column);
                item[key] = value;
            }

            return item;
        }

        static object GetValueObj(string value, TableColumn column)
        {
            if (column.type == "uniqueidentifier")
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (Guid?)null;
                }
                return Guid.Parse(value);
            }
            if (column.type == "bigint")
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (long?)null;
                }
                return long.Parse(value);
            }
            if (column.type == "int")
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (int?)null;
                }
                return int.Parse(value);
            }
            if (column.type == "smallint" || column.type == "bit")
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (short?)null;
                }
                return short.Parse(value);
            }

            if (new string[] {
                "decimal",
                "float",
                "numeric",
                "money",
            }.Contains(column.type))
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (decimal?)null;
                }
                return decimal.Parse(value);
            }

            if (new string[] {
                "date",
                "datetime",
                "smalldatetime",
            }.Contains(column.type))
            {
                if (string.IsNullOrEmpty(value) && column.null_able)
                {
                    return (DateTime?)null;
                }
                return DateTime.Parse(value);
            }

            // 字符串类型
            if (string.IsNullOrEmpty(value) && column.null_able)
            {
                return (string)null;
            }
            return value;
        }

        public static async Task Import(string filePath, string tableName, int batchSize)
        {
            var constr = ConfigUtils.GetConnectionString();
            using var conn = new SqlConnection(constr);

            if (conn.State != ConnectionState.Open)
            {
                await conn.OpenAsync();
            }

            try
            {
                var columns = await GetColumns(conn, tableName);

                var sql = GetInsertSql(columns, tableName);

                var allList = ReadCsv(filePath);
                var glist = GroupList(allList, batchSize);

                int rowCount = 0;

                await conn.ExecuteAsync($"TRUNCATE TABLE [{tableName}]");

                foreach (var group in glist)
                {
                    using var trans = await conn.BeginTransactionAsync();

                    try
                    {
                        var addList = group.Select(t => GetInsertObj(t, columns)).ToArray();
                        await conn.ExecuteAsync(sql, addList, trans);
                        await trans.CommitAsync();
                        rowCount += addList.Length;
                    }
                    catch (Exception ex)
                    {
                        LogService.Warn($"Table {tableName} Import Error: {ex.Message}");
                        LogService.Error(ex);
                        await trans.RollbackAsync();
                    }

                    LogService.Info($"Start Import {tableName} {rowCount}");
                }

                LogService.Info("End Import " + tableName);
            }
            catch (Exception ex)
            {
                LogService.Error(ex);
                LogService.Warn($"Table {tableName} Import Failed: " + ex.Message);
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}
