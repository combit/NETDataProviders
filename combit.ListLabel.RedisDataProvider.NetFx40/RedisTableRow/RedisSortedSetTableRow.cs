using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace combit.Reporting.DataProviders
{
    internal class RedisSortedSetTableRow : RedisBaseTableRow
    {

        string _tableName;
        public override string TableName => _tableName;

        /// <summary>
        /// Creates a <see cref="RedisSortedSetTableRow"/> from the given <paramref name="entry"/>
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="entry">Entry that should be used to build the <see cref="RedisSortedSetTableRow"/></param>
        public RedisSortedSetTableRow(string tableName, SortedSetEntry entry)
        {
            _tableName = tableName;
            TableColumns.Add(new RedisTableColumn("Value", LlFieldType.Text, entry.Element));
            TableColumns.Add(new RedisTableColumn("Score", LlFieldType.Numeric, entry.Score, typeof(double)));
        }
    }
}
