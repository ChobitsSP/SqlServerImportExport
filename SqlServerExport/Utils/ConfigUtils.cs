using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;

namespace SqlServerExport
{
    public static class ConfigUtils
    {
        public static IConfigurationRoot GetConfig()
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
#if DEBUG
                    .AddJsonFile("appsettings.Development.json")
#else
                    .AddJsonFile("appsettings.json")
#endif
                    .Build();

            return configuration;
        }

        /// <summary>
        /// 获取数据库连接字符串
        /// </summary>
        public static string GetConnectionString(string name = "Default")
        {
            var key = "ConnectionStrings" + name;
            var constr = Environment.GetEnvironmentVariable(key);
            if (!string.IsNullOrEmpty(constr)) return constr;
            constr = GetConfig().GetConnectionString(name);
            return constr;
        }

        public static string GetSectionValue(string name, string defVal = null)
        {
            return GetConfig().GetSectionValue(name, defVal);
        }

        public static int? GetInt(string name)
        {
            var str = GetSectionValue(name);
            if (string.IsNullOrEmpty(str)) return null;
            return int.TryParse(str, out var result) ? result : null;
        }

        public static string GetSectionValue(this IConfiguration config, string name, string defVal = null)
        {
            return config.GetSection(name)?.Value ?? defVal;
        }
    }
}
