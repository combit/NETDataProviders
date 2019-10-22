using System.Collections.Generic;
using System.Linq;
using combit.ListLabel25.DataProviders;
using StackExchange.Redis;

namespace combit.ListLabel25.RedisDataProvider
{
    public class RedisHashTable : RedisBaseTable
    {
        RedisDataProvider _provider;
        string _tableName;
        List<string> _columns;
        internal int _count;
               
        public RedisHashTable(string tableName, RedisKey key, RedisDataProvider provider)
        {
            Key = key;
            _provider = provider;
            _tableName = tableName;
            _count = 1; //always starts with 1 (RedisKey is always the first Row)
            _columns = GetColumns(key);
        }

        private List<string> GetColumns(RedisKey key)
        {
            List<string> list = new List<string>();
            HashEntry[] entries = _provider._database.HashGetAll(key);
            foreach (HashEntry entry in entries)
            {
                list.Add(entry.Name);
            }
            return list;
        }

        public override string TableName => _tableName;

        public override int Count => _count;

        public override IEnumerable<ITableRow> Rows
        {
            get
            {
                foreach (RedisKey key in _provider._server.Keys())
                {
                    if(_provider._database.KeyType(key) == RedisType.Hash)
                    {
                        var table = _provider.RegisteredHashTables.FirstOrDefault((predicate) => predicate(key) == _tableName);
                        if(table != null)
                        {
                            HashEntry[] hashEntries = _provider._database.HashGetAll(key);
                            yield return new RedisHashTableRow(_tableName, hashEntries, _columns);
                        }
                    }
                }
            }
        }

    }
}