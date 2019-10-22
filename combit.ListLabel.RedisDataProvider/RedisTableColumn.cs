using System;
using combit.ListLabel25;
using combit.ListLabel25.DataProviders;

namespace combit.ListLabel25.RedisDataProvider
{
    public class RedisTableColumn : ITableColumn
    {

        public RedisTableColumn(string columnName, LlFieldType type, object value)
            : this(columnName, type, value, typeof(string))
        {

        }

        public RedisTableColumn(string columnName, LlFieldType type, object value, Type dataType)
        {
            ColumnName = columnName;
            FieldType = type;
            Content = value;
            DataType = dataType;
        }

        public string ColumnName { get; private set; }

        public Type DataType { get; private set; }

        public object Content { get; private set; }

        public LlFieldType FieldType { get; private set; }
    }
}