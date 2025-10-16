using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using combit.Reporting.DataProviders;
using StackExchange.Redis;

namespace combit.Reporting.DataProviders
{

    /// <summary>
    /// Provider for Redis using https://github.com/StackExchange/StackExchange.Redis
    /// </summary>
    [Serializable]
    public sealed class RedisDataProvider : IDataProvider, IDisposable, ISerializable
    {

        #region Fields

        bool _initialized;
        string _multiplexerConfiguration;
        int _databaseId;
        internal IConnectionMultiplexer _connection;
        internal IDatabase _database;
        internal IServer _server;
        List<ITable> _tables;
        internal bool _disposed;

        /// <summary>
        /// Holds the user-registered HashTables and their filter.
        /// Return the tablename if your filter applies for the given string or null/String.Empty if it doesnt apply
        /// </summary>
        /// <example>
        /// provider.RegisteredHashTables.Add((s) =>
        /// {
        /// if (s.StartsWith("user"))
        ///         return "user";
        ///     return null;
        /// });
        /// </example>
        public List<Func<string, string>> RegisteredHashTables { get; } = new List<Func<string, string>>();

        #endregion

        #region Properties

        bool IDataProvider.SupportsAnyBaseTable => true;

        ReadOnlyCollection<ITable> IDataProvider.Tables
        {
            get
            {
                Init();
                return _tables.AsReadOnly();
            }
        }

        ReadOnlyCollection<ITableRelation> IDataProvider.Relations => null;

        #endregion

        #region Constructor

        private RedisDataProvider(SerializationInfo info, StreamingContext context)
        {
            _multiplexerConfiguration = info.GetString("MultiplexerConfiguration");
            _databaseId = info.GetInt32("DatabaseId");
        }

        /// <summary>
        /// Creates a new <see cref="RedisDatabase"/> instance
        /// </summary>
        /// <param name="multiplexerConfiguration">The string configuration for the multiplexer</param>
        public RedisDataProvider(string multiplexerConfiguration)
        {
            if (String.IsNullOrEmpty(multiplexerConfiguration))
                throw new ArgumentNullException(nameof(multiplexerConfiguration));

            ConfigurationOptions options = ConfigurationOptions.Parse(multiplexerConfiguration);
            _multiplexerConfiguration = multiplexerConfiguration;
            _databaseId = options.DefaultDatabase ?? -1;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Initializes the connection and loads the schema information
        /// </summary>
        /// <seealso cref="LoadSchema"/>
        void Init()
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(_connection));

            if (_initialized)
                return;

            _tables = new List<ITable>();
            _connection = ConnectionMultiplexer.Connect(_multiplexerConfiguration);
            _database = _connection.GetDatabase(_databaseId);
            _server = _connection.GetServer(_connection.GetEndPoints().First()); // we need the server because we need to scan all keys

            LoadSchema();

            _initialized = true;
        }

        /// <summary>
        /// Load all usable tables in <see cref="_tables"/>
        /// </summary>
        private void LoadSchema()
        {
            foreach (RedisKey key in _server.Keys())
            {
                switch (_database.KeyType(key))
                {
                    case RedisType.List:
                        _tables.Add(new RedisListTable(key, this));
                        break;
                    case RedisType.Set:
                        _tables.Add(new RedisSetTable(key, this));
                        break;
                    case RedisType.SortedSet:
                        _tables.Add(new RedisSortedSetTable(key, this));
                        break;
                    case RedisType.Hash:
                        //find a registered HashTable for the current key
                        var table = RegisteredHashTables.FirstOrDefault((predicate) => !string.IsNullOrEmpty(predicate(key)));
                        if (table != null)
                        {
                            //Key is registered
                            string tableName = table(key);
                            ITable currentHashTable = _tables.FirstOrDefault((s) => s.TableName == tableName);
                            if (currentHashTable != null && currentHashTable is RedisHashTable redisHashTable)
                            {
                                redisHashTable._count ++;
                            }
                            else
                            {
                                _tables.Add(new RedisHashTable(tableName, key, this)); //initialize HashTable
                            }
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("RedisDataProvider.Version", 1);
            info.AddValue("MultiplexerConfiguration", _multiplexerConfiguration);
            info.AddValue("DatabaseId", _databaseId);
            info.AddValue("RegisteredHashTables", RegisteredHashTables);
        }

        ITableRelation IDataProvider.GetRelation(string relationName)
        {
            return null;
        }

        ITable IDataProvider.GetTable(string tableName)
        {
            return _tables.FirstOrDefault(t => t.TableName == tableName);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        void Dispose(bool disposing)
        {
            if (_disposed)
                return;

            if (disposing && _connection != null)
                _connection.Dispose();

            _disposed = true;
        }

        ~RedisDataProvider() => Dispose(false); 

        #endregion

    }
}
