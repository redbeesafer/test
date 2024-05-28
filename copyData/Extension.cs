using Newtonsoft.Json.Linq;

namespace copyData
{
    /// <summary>
    /// 擴充功能
    /// </summary>
    public static class Extension
    {
        /// <summary>
        /// 取得dict的值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dict"></param>
        /// <param name="key"></param>
        /// <returns></returns>
        public static T GetValue<T>(this IDictionary<string, object> dict, string key)
        {
            try
            {
                if (dict.ContainsKey(key) && dict[key] != null)
                    return (T)Convert.ChangeType(dict[key], typeof(T));
            }
            catch
            {
                //error....
            }
            return default(T);
        }
    }
}
