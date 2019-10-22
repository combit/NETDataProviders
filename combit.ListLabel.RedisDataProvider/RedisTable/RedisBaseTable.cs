using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using combit.ListLabel25.DataProviders;
using StackExchange.Redis;

namespace combit.ListLabel25.RedisDataProvider
{
    public abstract class RedisBaseTable : ITable
    {

        internal RedisKey Key { get; set; }

        public virtual bool SupportsCount => true;

        public virtual bool SupportsSorting => false;

        public virtual bool SupportsAdvancedSorting => false;

        public virtual bool SupportsFiltering => false;

        public abstract int Count { get; }

        public virtual string TableName => Key;

        public abstract IEnumerable<ITableRow> Rows { get; }

        public virtual ReadOnlyCollection<string> SortDescriptions => throw new NotImplementedException();

        public virtual ITableRow SchemaRow => Rows.FirstOrDefault();

        public virtual void ApplyFilter(string filter)
        {
            throw new NotImplementedException();
        }

        public virtual void ApplySort(string sortDescription)
        {
            throw new NotImplementedException();
        }
    }
}
