﻿using System.Collections.Generic;
using combit.Reporting.DataProviders;
using StackExchange.Redis;

namespace combit.Reporting.RedisDataProvider
{
    public class RedisSetTable : RedisBaseTable
    {

        RedisDataProvider _provider;

        public RedisSetTable(RedisKey key, RedisDataProvider provider)
        {
            Key = key;
            _provider = provider;
        }

        public override int Count => (int)_provider._database.SetLength(Key);
        
        public override IEnumerable<ITableRow> Rows
        {
            get
            {
                RedisValue[] setMembers = _provider._database.SetMembers(Key);
                foreach (RedisValue item in setMembers)
                {
                    yield return new RedisValueTableRow(Key, item);
                }
            }
        }

    }
}
