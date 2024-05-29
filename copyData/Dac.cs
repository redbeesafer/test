using Dapper;
using Newtonsoft.Json;
using System.Data;
using System.Data.SqlClient;
using System.Transactions;

namespace copyData
{
    internal class Dac
    {
        private readonly Dictionary<string, object> _jsonDictionary = new Dictionary<string, object>();

        /// <summary>連線字串(來源)</summary>
        //private readonly string _strConnSource = @"Data Source=20.89.17.158;Initial Catalog=AIMUAT-IROO;User ID=aimcdpprj-irooadmin;Password=vm_sand0z;Encrypt=False;";
        private readonly string _strConnSource;

        /// <summary>連線字串(目的地)</summary>
        private readonly string _strConnDest;

        /// <summary>cmdTimeOut</summary>
        private const int _cmdTimeOut = 30;

        /// <summary>欄位(來源、目的地)</summary>
        private readonly string[] _sourceCols, _destCols;

        /// <summary>資料表名稱(來源、目的地)</summary>
        private readonly string _sourceTable, _destTable;

        /// <summary>總資料筆數</summary>
        private readonly int _totalDataCnt;

        /// <summary>批次筆數summary>
        private readonly int _pageSize;

        /// <summary>排序欄位(逗點分開)<summary>
        private readonly string _orderBy;

        /// <summary>自定義sql語法</summary>
        protected string _sql { get; set; }

        /// <summary>自定義條件</summary>
        protected object _objParam { get; set; }

        /// <summary>
        /// 建構子
        /// </summary>
        public Dac()
        {
            string jsonFilePath = Path.Combine(System.AppDomain.CurrentDomain.BaseDirectory, "config.json");
            string jsonString = File.ReadAllText(jsonFilePath);
            _jsonDictionary = JsonConvert.DeserializeObject<Dictionary<string, object>>(jsonString);

            _strConnSource = _jsonDictionary.GetValue<string>("SourceConn");
            _strConnDest = _jsonDictionary.GetValue<string>("DestConn");
            _sourceTable = _jsonDictionary.GetValue<string>("SourceTable");
            _destTable = _jsonDictionary.GetValue<string>("DestTable");
            _pageSize = _jsonDictionary.GetValue<int>("PageSize");

            _totalDataCnt = GetCnt();
            _sourceCols = GetCols();
            _destCols = GetCols(2);

            //排序欄位，有設定給預設值
            string orderBy = _jsonDictionary.GetValue<string>("OrderBy");
            _orderBy = !string.IsNullOrWhiteSpace(orderBy) ? orderBy : string.Join(',', GetCols(1));
        }

        /// <summary>
        /// 取得總資料筆數
        /// </summary>
        /// <returns></returns>
        protected int GetCnt()
        {
            _sql = $" select count(*) cnt from {_sourceTable} ";

            _objParam = null;

            return GetDataList<int>().First();
        }

        /// <summary>
        /// 取得資料表欄位
        /// </summary>
        /// <param name="type">
        /// 0 => 來源
        /// 1 => 來源PK
        /// 2 => 目的地
        /// </param>
        /// <returns></returns>
        protected string[] GetCols(int type = 0)
        {
            bool isSource = type == 0 || type == 1;
            string tableName = isSource ? _sourceTable : _destTable;

            switch (type)
            {
                case 1:
                    //抓pk
                    _sql = $"select lower(column_name) from information_schema.key_column_usage where table_name = '{tableName}'";
                    break;
                default:
                    //取得所有欄位(排除pk、identity)
                    _sql = $@" 
                        declare @obj_id int  = object_id('{tableName}');
                        with pkCTE as (
                        	select ic.object_id, ic.column_id
                        	from sys.indexes i
                        	inner join sys.index_columns ic on i.object_id = ic.object_id and i.index_id = ic.index_id
                        	where i.is_primary_key = 1 and i.object_id = @obj_id
                        )
                        select lower(c.name) as column_name
                        from sys.columns c
                        where c.object_id = @obj_id 
                        and c.is_identity = 0
                        and c.column_id not in (
                            select column_id from pkCTE
                        ) 
                     ";
                    break;
            }

            _objParam = null;
            return GetDataList<string>(isSource).ToArray();
        }


        /// <summary>
        /// 查詢(分頁)
        /// </summary>
        /// <param name="pageIndex">頁碼</param>
        /// <param name="pageSize">頁筆數</param>
        /// <param name="orderBy">依什麼排序</param>
        /// <returns></returns>
        public List<IDictionary<string, object>> GetDataList(int pageIndex, int pageSize, string orderBy)
        {
            _sql = $@" select {string.Join(',', _sourceCols)}
                       from {_sourceTable} (nolock)
                       order by {orderBy}
                       offset (@pageIndex - 1) * @pageSize rows
                       fetch next @pageSize rows only ";

            _objParam = new { pageIndex, pageSize };

            return GetDataList<IDictionary<string, object>>();
        }

        /// <summary>
        /// 新增目的地資料
        /// </summary>
        /// <param name="tableName">資料表</param>
        /// <param name="dataList"> 資料</param>
        /// <returns></returns>
        public int AddDataList(List<IDictionary<string, object>> dataList)
        {
            //整理出一樣的欄位
            string[] cols = _destCols.Where(col => _sourceCols.Contains(col)).ToArray();
            _sql = $@" insert {_destTable}
                           ({string.Join(',', cols)})
                       values
                           ({string.Join(",", cols.Select(col => $"@{col}"))}) ";

            _objParam = dataList;

            using (SqlConnection sqlConn = new SqlConnection(_strConnDest))
            {
                return sqlConn.Execute(_sql, _objParam, commandTimeout: _cmdTimeOut);
            }
        }

        /// <summary>
        /// 執行
        /// </summary>
        public void Run()
        {
            int totalCntNow = 0; //目前筆數
            int pageIndexMax = _pageSize == 0 ? 0 : (_totalDataCnt / _pageSize) + 1;  //最大頁碼
            string tiltle = "";
            List<IDictionary<string, object>> dataList = new List<IDictionary<string, object>>();

            for (int pageIndex = 1; pageIndex <= pageIndexMax; pageIndex++)
            {
                dataList = GetDataList(pageIndex, _pageSize, _orderBy);
                tiltle = $"第{(pageIndex - 1) * _pageSize + 1}筆 ~ 第{pageIndex * _pageSize}筆";
                try
                {
                    using (TransactionScope scope = new TransactionScope())
                    {
                        //totalCntNow += AddDataList(dataList);
                        Console.WriteLine($"{tiltle} 已匯入");
                        scope.Complete();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"{tiltle} 匯入失敗：{ex.Message + ex.StackTrace}");
                }
            }
            Console.WriteLine($"共匯入 {totalCntNow} 筆資料");
        }

        /// <summary>
        /// 取得來源資料(T)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="isSource">是否為來源</param>
        /// <returns></returns>
        protected List<T> GetDataList<T>(bool isSource = true)
        {
            string strConn = isSource ? _strConnSource : _strConnDest;
            using (SqlConnection sqlConn = new SqlConnection(strConn))
            {
                if (typeof(T) == typeof(IDictionary<string, object>) || typeof(T) == typeof(Dictionary<string, object>))
                {
                    IDictionary<string, object> dict = null;
                    var dictList = sqlConn.Query<object>(_sql, _objParam, commandTimeout: _cmdTimeOut).
                                        Select(data =>
                                        {
                                            if (data != null)
                                            {
                                                dict = (IDictionary<string, object>)data;
                                                foreach (string key in dict.Keys)
                                                {
                                                    if (dict[key] != null && typeof(byte[]) == dict[key].GetType())
                                                        dict[key] = "<<被加密囉>>"; //處理byte[]的部分....
                                                }
                                            }
                                            return data == null ? default(T) : (T)dict;
                                        }).ToList();
                    return dictList;
                }
                else
                    return sqlConn.Query<T>(_sql, _objParam, commandTimeout: _cmdTimeOut).ToList();
            }
        }
    }
}
