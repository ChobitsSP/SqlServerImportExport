using SqlServerImport.Utils;
using System.Text;

namespace SqlServerImport
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            LogService.Init();

            var folder = GetFolder();

            var batchSize = ConfigUtils.GetInt("Import:BatchSize") ?? 10000;

            var csvFiles = Directory.GetFiles(folder, "*.csv", SearchOption.TopDirectoryOnly);

            foreach (var csvFile in csvFiles)
            {
                var fileName = Path.GetFileNameWithoutExtension(csvFile);

                Console.WriteLine($"Importing {fileName}...");

                try
                {
                    await BulkCopyImport.ImportData(csvFile, fileName, batchSize);
                }
                catch (Exception ex)
                {
                    LogService.Error(ex);
                    Console.WriteLine($"Error importing {fileName}: {ex.Message}");
                    continue;
                }

                Console.WriteLine($"Import {fileName} completed.");
            }

            Console.WriteLine($"Import completed.");
            Console.ReadLine();
        }

        static string GetFolder()
        {
            var folder = ConfigUtils.GetSectionValue("Import:Folder");
            if (!string.IsNullOrEmpty(folder) && Directory.Exists(folder)) return folder;

            while (true)
            {
                Console.WriteLine("Please input folder:");
                folder = Console.ReadLine();

                if (!Directory.Exists(folder))
                {
                    Console.WriteLine("Folder not exists, please input again.");
                    continue;
                }

                return folder;
            }
        }
    }
}
