using System.Collections.Generic;
using combit.ListLabel25.DataProviders;
using StackExchange.Redis;

namespace combit.ListLabel25.RedisDataProvider
{
    public class RedisListTable : RedisBaseTable
    {

        RedisDataProvider _provider;

        public RedisListTable(RedisKey key, RedisDataProvider provider)
        {
            Key = key;
            _provider = provider;
        }

        public override int Count => (int)_provider._database.ListLength(Key);

        public override IEnumerable<ITableRow> Rows
        {
            get
            {
                RedisValue[] setMembers = _provider._database.ListRange(Key);
                foreach (RedisValue item in setMembers)
                {
                    yield return new RedisValueTableRow(Key, item);
                }
            }
        }
    }
}
