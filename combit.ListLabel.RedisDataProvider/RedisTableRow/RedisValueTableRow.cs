using System.Collections.Generic;
using System.Collections.ObjectModel;
using combit.ListLabel24;
using combit.ListLabel24.DataProviders;
using StackExchange.Redis;

namespace combit.ListLabel24.RedisDataProvider
{
    public class RedisValueTableRow : RedisBaseTableRow
    {
        
        public RedisValueTableRow(RedisKey key, RedisValue item)
        {
            Key = key;
            TableColumns.Add(new RedisTableColumn("Value", LlFieldType.Text, item.ToString()));
        }

    }
}