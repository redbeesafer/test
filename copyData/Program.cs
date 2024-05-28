using System.Transactions;

namespace copyData
{
    /// <summary>
    /// 轉檔用
    /// </summary>
    internal class Program
    {
        static void Main(string[] args)
        {
            try
            {
                new Dac().Run();                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message + ex.StackTrace);
            }
            Console.ReadLine();
        }
    }
}
