using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace combit.Reporting.RedisDataProvider
{
    internal class RedisHashTableRow : RedisBaseTableRow
    {

        string _tableName;
        public override string TableName => _tableName;

        /// <summary>
        /// Creates a <see cref="RedisHashTableRow"/> from given <paramref name="hashEntries"/> for all <paramref name="columns"/> specified
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="hashEntries">All Entries that should be added as a column (only if <see cref="HashEntry.Name"/> exists in <paramref name="columns"/>)</param>
        /// <param name="columns">List of all needed columns. Non existing columns will be filled up with <c>null</c></param>
        public RedisHashTableRow(string tableName, HashEntry[] hashEntries, List<string> columns)
        {
            _tableName = tableName;
            foreach (HashEntry entry in hashEntries)
            {
                if(columns.Contains(entry.Name))
                    TableColumns.Add(new RedisTableColumn(entry.Name, LlFieldType.Text, entry.Value));
            }

            //add missing columns
            foreach (string col in columns)
            {
                if(!TableColumns.Any(s => s.ColumnName == col))
                    TableColumns.Add(new RedisTableColumn(col, LlFieldType.Text, null));
            }

        }

    }
}