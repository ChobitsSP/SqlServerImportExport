using System.Text;

namespace SqlServerExport
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
            LogService.Init();

            try
            {
                await DapperExport.BackupToCsv();
            }
            catch (Exception ex)
            {
                LogService.Error(ex);
            }
        }
    }
}
