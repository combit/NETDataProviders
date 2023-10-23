using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using combit.Reporting.DataProviders;
using combit.Reporting.RedisDataProvider.RedisTableRow;
using StackExchange.Redis;

namespace combit.Reporting.RedisDataProvider
{
    public class RedisSortedSetTable : RedisBaseTable
    {
        RedisDataProvider _provider;
        string _sortDescription = "";

        public RedisSortedSetTable(RedisKey key, RedisDataProvider provider)
        {
            Key = key;
            _provider = provider;
        }

        public override int Count => (int)_provider._database.SortedSetLength(Key);

        public override IEnumerable<ITableRow> Rows
        {
            get
            {
                SortedSetEntry[] setMembers;
                Order order = _sortDescription.EndsWith("DESC") ? Order.Descending : Order.Ascending;
                string orderBy = _sortDescription.Replace("ASC", "").Replace("DESC", "").Trim();

                //apply sorting
                switch (orderBy.ToUpperInvariant())
                {
                    case "BY RANK":
                        setMembers = _provider._database.SortedSetRangeByRankWithScores(Key, order: order);
                        break;
                    case "BY SCORE":
                        setMembers = _provider._database.SortedSetRangeByScoreWithScores(Key, order: order);
                        break;
                    default:
                        setMembers = _provider._database.SortedSetRangeByRankWithScores(Key, order: order);
                        break;
                }
                
                foreach (SortedSetEntry item in setMembers)
                {
                    yield return new RedisSortedSetTableRow(Key, item);   
                }
            }
        }

        public override bool SupportsSorting => true;

        public override ReadOnlyCollection<string> SortDescriptions 
            => new List<string>() {
                "By Rank [+]", "By Rank [-]",
                "By Score [+]", "By Score [-]",
            }.AsReadOnly();

        public override void ApplySort(string sortDescription)
        {
            _sortDescription = sortDescription.Replace("[+]", "ASC").Replace("[-]", "DESC");
        }
    }
}