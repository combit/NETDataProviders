using System.Collections.Generic;
using System.Collections.ObjectModel;
using combit.Reporting;
using combit.Reporting.DataProviders;
using StackExchange.Redis;

namespace combit.Reporting.DataProviders
{
    internal class RedisValueTableRow : RedisBaseTableRow
    {
        
        public RedisValueTableRow(RedisKey key, RedisValue item)
        {
            Key = key;
            TableColumns.Add(new RedisTableColumn("Value", LlFieldType.Text, item.ToString()));
        }

    }
}