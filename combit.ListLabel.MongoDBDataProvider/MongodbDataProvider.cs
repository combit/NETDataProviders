using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provider for MongoDB, requires MongoDB Inc. NuGet packages
    /// </summary>
    public sealed class MongoDBDataProvider : IDataProvider, ICanHandleUsedIdentifiers, ISupportsLogger, ISupplyBaseTables
    {
        private bool _initialized;
        private ILlLogger _logger;

        public MongoClient Client { get; set; }
        private string DatabaseName;
        public IMongoDatabase Database;

        private List<ITable> _tables = new List<ITable>();
        private List<string> _tableNames = new List<string>();

        internal List<ITableRelation> _relations = new List<ITableRelation>();
        internal Dictionary<string, BsonDocument> tableSchemas = new Dictionary<string, BsonDocument>();


        public bool SupportCount { get; set; }

        /// <summary>
        /// Constractor of MongoDB data provider for ListLabel
        /// </summary>
        /// <param name="serverAddress">Server address of MongoDB server</param>
        /// <param name="databaseName">Name of the database</param>
        /// <param name="username">Username to connect to the MongoDB server</param>
        /// <param name="password">password to connect to the MongoDB server</param>
        /// <param name="port">Port of the MongoDB server. Default MongoDB port is 27017, so just change it if another port number is explicitly defined in MongoDB server</param>
        public MongoDBDataProvider(string serverAddress, string databaseName, string username, string password, string port = "27017")
        {
            if (username != null && username != string.Empty && password != null && password != string.Empty)
            {
                var credential = MongoCredential.CreateCredential(databaseName, username, password);
                var server = new MongoServerAddress(serverAddress, int.Parse(port));
                var clientSettings = new MongoClientSettings()
                {
                    Credential = credential,
                    Server = server
                };
                Client = new MongoClient(clientSettings);
                CheckAuthentication(databaseName, username, password, new MongoServerAddress(serverAddress, int.Parse(port)));
            }
            else
            {
                Client = new MongoClient(string.Format("mongodb://{0}:{1}/{2}", serverAddress, port, databaseName));
            }
            DatabaseName = databaseName;
            SupportCount = true;
        }

        private void CheckAuthentication(string databaseName, string username, string password, MongoServerAddress server)
        {
            try
            {
                var credential = MongoCredential.CreateCredential(databaseName, username, password);
                var clientSettings = new MongoClientSettings()
                {
                    Credential = credential,
                    WaitQueueTimeout = new TimeSpan(0, 0, 0, 5),
                    ConnectTimeout = new TimeSpan(0, 0, 0, 30),
                    Server = server,
                    ClusterConfigurator = builder =>
                    {
                        //The "Normal" Timeout settings are for something different. This one here really is relevant when it is about
                        //how long it takes until we stop, when we cannot connect to the MongoDB Instance                        
                        builder.ConfigureCluster(settings => settings.With(serverSelectionTimeout: TimeSpan.FromSeconds(1)));
                    }
                };

                var mongoClient = new MongoClient(clientSettings);
                var testDB = mongoClient.GetDatabase(databaseName);
                var cmd = new BsonDocument("count", "foo");

                var result = testDB.RunCommand<BsonDocument>(cmd);
            }
            catch (TimeoutException ex)
            {
                if (ex.Message.Contains("Authentication failed"))
                {
                    Logger.Error(LogCategory.DataProvider, "Unable to authenticate username '{0}' on database '{1}'.\nError message:\n{2}", username, databaseName, ex.Message);
                    throw new ListLabelException(string.Format("Authentication to {0} failed!", databaseName));
                }
            }
        }

        // these are the possible root tables (i.e. no relation children)
        private List<string> _baseTables = new List<string>();

        private void Init()
        {
            if (_initialized)
                return;

            _initialized = true;
            IAsyncCursor<BsonDocument> collections = null;
            try
            {
                Database = Client.GetDatabase(DatabaseName);
                collections = Database.ListCollections();
            }
            catch (TimeoutException ex)
            {
                Logger.Error(LogCategory.DataProvider, "Error while trying to list collections of database.\nError message:\n{0}", ex.Message);
                throw new ListLabelException(string.Format("A timeout occured: Unable to connect to Server: {0}", Client.Settings.Server));
            }
            catch (Exception ex)
            {
                Logger.Error(LogCategory.DataProvider, "Error while trying to list collections of database.\nError message:\n{0}", ex.Message);
                throw;
            }
            foreach (var collection in collections.ToEnumerable())
            {
                var systemCollections = new string[4] { "system.namespaces", "system.indexes", "system.profile", "system.js" };
                if (!systemCollections.Contains(collection[0].ToString()))
                {
                    var data = Database.GetCollection<BsonDocument>(collection[0].ToString());
                    var table = new MongoDBTable(collection[0].ToString(), this, true, null, data);
                    _tables.Add(table);
                    _tableNames.Add(table.TableName);
                    _baseTables.Add(table.TableName);
                    BuildTableStructure(true, data, table.TableName);
                }
            }
        }

        internal ILlLogger Logger { get { return _logger ?? LoggingHelper.DummyLogger; } }
        public void SetLogger(ILlLogger logger, bool overrideExisting)
        {
            if (_logger == null || overrideExisting)
            {
                _logger = logger;
            }
        }

        private void BuildTableStructure(bool isCollection, IMongoCollection<BsonDocument> data, string parentName, BsonArray arrayData = null)
        {
            foreach (var row in isCollection ? data.AsQueryable().ToList() : arrayData.Values.Select(v => v as BsonDocument).ToList())
            {
                foreach (var column in row)
                {
                    if (column.Value.IsBsonArray)
                    {
                        string tableName = string.Format("Array_{0}_{1}", parentName, column.Name);
                        if (!_tableNames.Contains(tableName))
                        {
                            var tableData = column.Value.AsBsonArray;
                            tableSchemas.Add(tableName, tableData[0].ToBsonDocument());
                            var table = new MongoDBTable(tableName, this, false, tableData, null);
                            _tables.Add(table);
                            _tableNames.Add(table.TableName);
                            var relation = new MongoDBTableRelation(parentName, column.Name, tableName, column.Name, string.Format("{0}2{1}", parentName, tableName));
                            _relations.Add(relation);
                            BuildTableStructure(false, null, tableName, tableData);
                        }
                    }
                }
            }
        }

        // make sure to always operate on the most recent table instance - otherwise
        // native aggregates may return wrong results for sub tables.
        internal void ReplaceTableInstance(string tableName, ITable table)
        {
            ITable tableToRemove = null;
            foreach (ITable tableInstance in _tables)
            {
                if (tableInstance.TableName == tableName)
                {
                    tableToRemove = tableInstance;
                    break;
                }
            }

            if (tableToRemove != null)
                _tables.Remove(tableToRemove);

            _tables.Add(table);
        }

        bool IDataProvider.SupportsAnyBaseTable { get { return false; } }

        public ReadOnlyCollection<ITable> Tables
        {
            get
            {
                Init();
                return _tables.AsReadOnly();
            }
        }

        public ITable GetTable(string tableName)
        {
            Init();
            foreach (ITable list in _tables)
            {
                if (list.TableName == tableName)
                    return list;
            }

            return null;
        }

        ITableRelation IDataProvider.GetRelation(string relationName)
        {
            Init();
            foreach (ITableRelation relation in _relations)
            {
                if (relation.RelationName == relationName)
                    return relation;
            }
            return null;
        }
        ReadOnlyCollection<ITableRelation> IDataProvider.Relations
        {
            get
            {
                Init();
                return _relations.AsReadOnly();
            }
        }


        void ICanHandleUsedIdentifiers.SetUsedIdentifiers(ReadOnlyCollection<string> identifiers)
        {
            UsedIdentifierHelper helper = new UsedIdentifierHelper(identifiers);

            foreach (ITable table in _tables)
            {
                ReadOnlyCollection<string> usedHere = helper.GetIdentifiersForTable(this, table.TableName, false);

                if (usedHere.Count > 0)
                    (table as ICanHandleUsedIdentifiers).SetUsedIdentifiers(usedHere);
            }
        }

        internal static string ListToSeparatedString(string separator, ICollection<string> list)
        {
            string[] arr = new string[list.Count];
            list.CopyTo(arr, 0);
            return String.Join(separator, arr);
        }

        public ReadOnlyCollection<string> GetBaseTableList()
        {
            return _baseTables.AsReadOnly();
        }
    }

    internal class MongoDBTable : ITable, ICanHandleUsedIdentifiers
    {
        private MongoDBDataProvider _provider;
        private string _whereClause = null;
        private string _orderClause = null;
        private IMongoCollection<BsonDocument> _collectionData = null;
        private BsonArray _arrayData = null;

        internal ReadOnlyCollection<string> UsedIdentifiers { get; private set; }



        public MongoDBTable(string tableName, MongoDBDataProvider provider, bool isCollection, BsonArray arrayData, IMongoCollection<BsonDocument> collectionData)
        {
            TableName = tableName;
            _provider = provider;
            _collectionData = collectionData;
            _arrayData = arrayData;

            if (provider.SupportCount)
            {
                Count = isCollection ? int.Parse(collectionData.CountDocuments(_ => true).ToString()) : _arrayData != null ? arrayData.Count : 0;
            }
            if (_collectionData != null || _arrayData != null)
            {
                var firstRow = isCollection ? collectionData.Find(_ => true).First() : arrayData?[0].AsBsonDocument;
                SchemaRow = new MongoDBTableRow(this, _provider, firstRow, UsedIdentifiers);
            }
            else
            {
                SchemaRow = new MongoDBTableRow(this, _provider, _provider.tableSchemas[TableName], UsedIdentifiers);
            }
        }

        #region ITable Members
        public bool SupportsCount { get { return _provider.SupportCount; } }

        public bool SupportsSorting { get { return true; } }
        public bool SupportsAdvancedSorting { get { return true; } }
        public bool SupportsFiltering { get { return true; } }

        public void ApplySort(string sortDescription)
        {
            _orderClause = sortDescription;
        }

        public void ApplyFilter(string filter)
        {
            _whereClause = filter;
        }

        public int Count { get; private set; }

        public string TableName { get; private set; }

        public IEnumerable<ITableRow> Rows
        {
            get
            {
                return GetRows(false);
            }
        }

        public IEnumerable<ITableRow> GetRows(bool init)
        {
            if (_collectionData != null)
            {
                var filter = new BsonDocument();
                if (_whereClause != null && _whereClause != string.Empty)
                {
                    string[] filterDef = _whereClause.Split('=');
                    filter = new BsonDocument(filterDef[0], filterDef[1]);
                }
                SortDefinition<BsonDocument> sort = null;
                if (_orderClause != null && _orderClause != string.Empty)
                {
                    StringBuilder sortBuilder = new StringBuilder("{ ");
                    string[] sorts = _orderClause.Split('\t');
                    for (int i = 0; i < sorts.Count(); i++)
                    {
                        sortBuilder.Append(sorts[i].Replace("[+]", ": 1").Replace("[-]", ": -1"));
                        sortBuilder.Append(i != sorts.Count() - 1 ? ", " : "");
                    }
                    sortBuilder.Append("}");
                    sort = sortBuilder.ToString();
                }

                //_orderClause = sort.ToString(); //sortDescription.Replace("\t", ",").Replace("[+]", "ASC").Replace("[-]", "DESC");
                List<BsonDocument> rows = new List<BsonDocument>();
                try
                {
                    rows = _provider.Database.GetCollection<BsonDocument>(TableName).Find(filter).Sort(sort).ToList();
                }
                catch (Exception ex)
                {
                    _provider.Logger.Error(LogCategory.DataProvider, "MongoDB: Error while executing filter:\n{0}\nsort:\n{1}\n\nDetails:\n{2}", filter, sort, ex.Message);
                    throw;
                }
                foreach (var row in rows)
                {
                    yield return new MongoDBTableRow(this, _provider, row, UsedIdentifiers);
                }
            }
            else if (_arrayData != null)
            {

                foreach (var row in _arrayData)
                {
                    yield return new MongoDBTableRow(this, _provider, row.AsBsonDocument, UsedIdentifiers);
                }
            }
        }

        public System.Collections.ObjectModel.ReadOnlyCollection<string> SortDescriptions
        {
            get
            {
                List<string> sortOrders = new List<string>();

                foreach (var column in SchemaRow.Columns)
                {
                    // add sort order
                    sortOrders.Add(String.Concat(column.ColumnName, " [+]"));
                    sortOrders.Add(String.Concat(column.ColumnName, " [-]"));
                }

                return sortOrders.AsReadOnly();
            }
        }

        public ITableRow SchemaRow { get; private set; }

        #endregion

        #region ICanHandleUsedIdentifiers
        public void SetUsedIdentifiers(ReadOnlyCollection<string> identifiers)
        {
            UsedIdentifiers = identifiers;
        }
        #endregion
    }

    internal class MongoDBTableRelation : ITableRelation
    {
        public string RelationName { get; set; }
        public string ParentColumnName { get; set; }
        public string ParentTableName { get; set; }
        public string ChildTableName { get; set; }
        public string ChildColumnName { get; set; }

        public MongoDBTableRelation(string parentTableName, string parentColumnName, string childTableName, string childColumnName, string relationName)
        {
            ParentTableName = parentTableName;
            ChildTableName = childTableName;
            RelationName = relationName;
            ParentColumnName = parentColumnName;
            ChildColumnName = childColumnName;
        }
    }

    internal class MongoDBTableRow : ITableRow
    {
        private List<ITableColumn> _columns;
        private MongoDBDataProvider _provider;
        private BsonDocument _data;
        private ReadOnlyCollection<string> _usedIdentifiers;
        private MongoDBTable _table;

        public MongoDBTableRow(MongoDBTable table, MongoDBDataProvider provider, BsonDocument data, ReadOnlyCollection<string> usedIdentifiers)
        {
            TableName = table.TableName;
            _provider = provider;
            _data = data;
            _usedIdentifiers = usedIdentifiers;
            _table = table;
        }

        private void InitColumns()
        {
            _columns = new List<ITableColumn>();

            foreach (var element in _data)
            {
                if (_provider._relations.Any(r => r.ParentTableName == TableName && r.ParentColumnName == element.Name))
                    continue;
                if (_usedIdentifiers == null || _usedIdentifiers.Contains(element.Name) || element.Value.IsBsonDocument)
                {
                    if (element.Value.IsBsonDocument)
                    {
                        foreach (var docElement in element.Value.AsBsonDocument)
                        {
                            if (_usedIdentifiers == null || _usedIdentifiers.Contains(string.Format("{0}.{1}", element.Name, docElement.Name)))
                                AddColumn(docElement, element.Name);
                        }
                    }
                    else
                    {
                        AddColumn(element);
                    }
                }
            }
        }

        private void AddColumn(BsonElement element, string docName = null)
        {
            string name = docName == null ? element.Name : string.Format("{0}.{1}", docName, element.Name);
            Type dataType = GetDataType(element.Value);
            if (_data != null)
                _columns.Add(new MongoDBTableColumn(name, dataType, element.Value, dataType.ToString()));
            else
                _columns.Add(new MongoDBTableColumn(name, dataType, GetDefaultValueForType(dataType.ToString(), "Sample Text"), dataType.ToString()));
        }

        public bool SupportsGetParentRow { get { return false; } }

        public string TableName { get; private set; }

        public ReadOnlyCollection<ITableColumn> Columns
        {
            get
            {
                if (_columns == null)
                    InitColumns();

                return _columns.AsReadOnly();
            }
        }

        public ITable GetChildTable(ITableRelation relation)
        {
            // Can happen if the master table is assigned as variables
            if (_columns == null)
                InitColumns();

            ITable result = null;
            var column = _data.Elements.Where(e => e.Name == relation.ParentColumnName).FirstOrDefault();
            var tableData = column.Value.IsBsonNull ? null : column.Value.AsBsonArray;
            result = new MongoDBTable(relation.ChildTableName, _provider, false, tableData, null);

            _provider.ReplaceTableInstance(relation.ChildTableName, result);
            return result;
        }

        public ITableRow GetParentRow(ITableRelation relation)
        {
            throw new NotImplementedException();
        }

        private object GetDefaultValueForType(string dataType, string stringDefaultValue)
        {
            switch (dataType)
            {
                case "System.String": return stringDefaultValue;
                case "System.Byte": return new byte();
                case "System.SByte": return new sbyte();
                case "System.Int16": return new short();
                case "System.Int32": return new int();
                case "System.Int64": return new long();
                case "System.Double": return new double();
                case "System.Single": return new float();
                case "System.Boolean": return new bool();
                case "System.Decimal": return new decimal();
                case "System.Guid": return new Guid();
                case "System.Date": return DateTime.Now.Date;
                case "System.TimeOfDay": return DateTime.Now.TimeOfDay;
                case "System.DateTimeOffset": return DateTime.Now.ToUniversalTime();
                case "System.Duration": return TimeSpan.Zero;
                case "System.DateTime": return DateTime.Now;
                case "System.Time": return new TimeSpan();

                default: return null;
            }
        }

        private Type GetDataType(BsonValue element)
        {
            if (element.IsBoolean)
            {
                return typeof(bool);
            }
            else if (element.IsValidDateTime)
            {
                return typeof(DateTime);
            }
            else if (element.IsDouble)
            {
                return typeof(double);
            }
            else if (element.IsGuid)
            {
                return typeof(Guid);
            }
            else if (element.IsInt32 || element.IsNumeric)
            {
                return typeof(Int32);
            }
            else if (element.IsInt64)
            {
                return typeof(Int64);
            }
            else
            {
                return typeof(string);
            }
        }
    }

    internal class MongoDBTableColumn : ITableColumn
    {
        public string ColumnName { get; private set; }
        public Type DataType { get; private set; }
        public object Content { get; private set; }
        public LlFieldType FieldType { get { return GetFieldTypeFromDataType(); } }
        public string MongoDBColumnType { get; private set; }

        public MongoDBTableColumn(string columnName, Type dataType, object content, string type)
        {
            ColumnName = columnName;
            DataType = dataType;
            Content = content.ToString() == "BsonNull" ? string.Empty : content;
            MongoDBColumnType = type;

            switch (dataType.Name)
            {
                case "IDictionary`2":
                    DataType = typeof(string);
                    List<string> keys = ((SortedDictionary<string, string>)content).Keys.ToList();
                    Content = MongoDBDataProvider.ListToSeparatedString(",", keys);
                    break;
                case "IEnumerable`1":
                    DataType = typeof(string);
                    Content = MongoDBDataProvider.ListToSeparatedString(",", content as List<string>);
                    break;
                default:
                    break;
            }
        }

        private LlFieldType GetFieldTypeFromDataType()
        {
            switch (DataType.FullName)
            {
                case "System.Boolean":
                    return LlFieldType.Boolean;
                case "System.DateTime":
                    return LlFieldType.Unknown;
                case "System.Double":
                    return LlFieldType.Numeric;
                case "System.Guid":
                    return LlFieldType.Unknown;
                case "System.Int32":
                    return LlFieldType.Numeric_Integer;
                case "System.Int64":
                    return LlFieldType.Numeric_Integer;
                default:
                    return LlFieldType.Unknown;
            }
        }
    }

}
