using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using combit.ListLabel24.DataProviders;
using StackExchange.Redis;

namespace combit.ListLabel24.RedisDataProvider
{
    public abstract class RedisBaseTableRow : ITableRow
    {

        internal RedisKey Key { get; set; }
        internal List<ITableColumn> TableColumns { get; set; } = new List<ITableColumn>();

        public virtual bool SupportsGetParentRow => false;

        public virtual string TableName => Key;

        public virtual ReadOnlyCollection<ITableColumn> Columns => TableColumns.AsReadOnly();

        public virtual ITable GetChildTable(ITableRelation relation)
        {
            throw new NotImplementedException();
        }

        public virtual ITableRow GetParentRow(ITableRelation relation)
        {
            throw new NotImplementedException();
        }
    }
}
