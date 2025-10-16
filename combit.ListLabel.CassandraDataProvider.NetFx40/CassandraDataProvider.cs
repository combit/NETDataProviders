using Cassandra;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Text.RegularExpressions;

#if LLCP
using combit.Logging;
#endif

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provides a data provider implementation for Apache Cassandra databases.
    /// </summary>
    /// <remarks>
    /// The <see cref="CassandraDataProvider"/> class implements <see cref="IDataProvider"/>, <see cref="ICanHandleUsedIdentifiers"/>, 
    /// and <see cref="ISupportsLogger"/> to allow access to data stored in Apache Cassandra. It uses the Cassandra .NET Driver 
    /// to connect to a Cassandra cluster and query metadata (such as table names) from the specified keyspace. This provider 
    /// supports count queries via the <see cref="SupportCount"/> property.
    /// </remarks>
    /// <example>
    /// The following example demonstrates how to use the <see cref="CassandraDataProvider"/>:
    /// <code language="csharp">
    /// // Create an instance of the CassandraDataProvider with the cluster address and keyspace.
    /// CassandraDataProvider provider = new CassandraDataProvider("127.0.0.1", "my_keyspace");
    /// 
    /// // Assign the provider as the data source for the List &amp; Label reporting engine.
    /// using ListLabel listLabel = new ListLabel();
    /// listLabel.DataSource = provider;
    /// ExportConfiguration exportConfiguration = new ExportConfiguration(LlExportTarget.Pdf, exportFilePath, projectFilePath);
    /// exportConfiguration.ShowResult = true;
    /// listLabel.Export(exportConfiguration);
    /// </code>
    /// </example>
    public sealed class CassandraDataProvider : IDataProvider, ICanHandleUsedIdentifiers, ISupportsLogger
    {
        private bool _initialized;
        private ILlLogger _logger;
#pragma warning disable CS3003
        /// <summary>
        /// Gets or sets the Cassandra session used for executing queries.
        /// </summary>
        public ISession CassandraSession { get; set; }
        /// <summary>
        /// Gets or sets the metadata of the connected Cassandra cluster.
        /// </summary>
        public Metadata CassandraMetadata { get; set; }
#pragma warning restore CS3003
        private string _clusterAddress;
        private string _keyspace;
        private List<ITable> _tables = new List<ITable>();

        /// <summary>
        /// Gets or sets a value indicating whether the provider supports count queries.
        /// </summary>
        public bool SupportCount { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CassandraDataProvider"/> class for the specified cluster address and keyspace.
        /// </summary>
        /// <param name="address">The address of the Cassandra cluster.</param>
        /// <param name="keyspace">The keyspace to connect to.</param>
        public CassandraDataProvider(string address, string keyspace)
        {
            _clusterAddress = address;
            _keyspace = keyspace;
            SupportCount = true;
        }

        /// <summary>
        /// Initializes the provider by connecting to the Cassandra cluster and retrieving table metadata.
        /// </summary>
        private void Init()
        {
            if (_initialized)
                return;

            _initialized = true;

            Cluster cluster = Cluster.Builder().AddContactPoint(_clusterAddress).Build();
            CassandraSession = cluster.Connect(_keyspace);
            CassandraMetadata = cluster.Metadata;
            try
            {
                // Query system.schema_columnfamilies to get the list of tables in the keyspace.
                RowSet result = CassandraSession.Execute(string.Format("select * from system.schema_columnfamilies where keyspace_name='{0}';", _keyspace));
                foreach (var table in result)
                {
                    _tables.Add(new CassandraTable(table["columnfamily_name"].ToString(), this));
                }
            }
            catch (Exception ex)
            {
                Logger.Error(LogCategory.DataProvider, "Error while querying system.schema_columnfamilies: {0}.", ex);
                throw;
            }
        }

        /// <summary>
        /// Gets the logger used by the data provider.
        /// </summary>
        internal ILlLogger Logger { get { return _logger ?? LoggingHelper.LlCoreDebugOutputLogger; } }

        /// <summary>
        /// Sets the logger for the data provider.
        /// </summary>
        /// <param name="logger">The logger instance to use.</param>
        /// <param name="overrideExisting">
        /// If set to <c>true</c>, any existing logger will be overridden; otherwise, the current logger remains unchanged.
        /// </param>
        void ISupportsLogger.SetLogger(ILlLogger logger, bool overrideExisting)
        {
            if (_logger == null || overrideExisting)
            {
                _logger = logger;
            }
        }

        /// <inheritdoc/>
        bool IDataProvider.SupportsAnyBaseTable { get { return true; } }

        /// <summary>
        /// Gets a read-only collection of tables available from the Cassandra keyspace.
        /// </summary>
        public ReadOnlyCollection<ITable> Tables
        {
            get
            {
                Init();
                return _tables.AsReadOnly();
            }
        }

        /// <summary>
        /// Retrieves a table by its name from the Cassandra keyspace.
        /// </summary>
        /// <param name="tableName">The name of the table to retrieve.</param>
        /// <returns>
        /// The <see cref="ITable"/> corresponding to the specified table name, or <c>null</c> if not found.
        /// </returns>
        ITable IDataProvider.GetTable(string tableName)
        {
            Init();
            foreach (ITable list in _tables)
            {
                if (list.TableName == tableName)
                    return list;
            }
            return null;
        }

        /// <inheritdoc/>
        ITableRelation IDataProvider.GetRelation(string relationName)
        {
            // CassandraDataProvider does not support relations.
            return null;
        }

        /// <inheritdoc/>
        ReadOnlyCollection<ITableRelation> IDataProvider.Relations
        {
            get { return null; }
        }

        /// <inheritdoc/>
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

        /// <summary>
        /// Converts a list of strings to a single string with items separated by the specified separator.
        /// </summary>
        /// <param name="separator">The string separator to use.</param>
        /// <param name="list">The collection of strings.</param>
        /// <returns>A string containing the list items separated by the specified separator.</returns>
        internal static string ListToSeparatedString(string separator, ICollection<string> list)
        {
            string[] arr = new string[list.Count];
            list.CopyTo(arr, 0);
            return String.Join(separator, arr);
        }

        /// <summary>
        /// Converts a dictionary of string pairs to a single string with key-value pairs separated by the specified separator.
        /// </summary>
        /// <param name="separator">The string separator to use between pairs.</param>
        /// <param name="dic">The dictionary to convert.</param>
        /// <returns>A string representation of the dictionary with key-value pairs separated by the specified separator.</returns>
        internal static string DictionaryToSeparatedPairs(string separator, IDictionary<string, string> dic)
        {
            return string.Join(separator, dic.Select(x => x.Key + ":" + x.Value).ToArray());
        }
    }

    internal class CassandraTable : ITable, IAdvancedFiltering, ICanHandleUsedIdentifiers
    {
        private CassandraDataProvider _provider;
        private string _whereClause = string.Empty;
        private string _orderClause = string.Empty;
        internal ReadOnlyCollection<string> UsedIdentifiers { get; private set; }
        private TableMetadata _cassandraTableMetadata;


        public CassandraTable(string tableName, CassandraDataProvider provider)
        {
            TableName = tableName;
            _provider = provider;

            _cassandraTableMetadata = _provider.CassandraMetadata.GetTable(_provider.CassandraSession.Keyspace, tableName);

            if (provider.SupportCount)
            {
                Row row = provider.CassandraSession.Execute(string.Format("select count(*) from {0};", tableName)).First();
                Count = int.Parse(row["count"].ToString());
            }

            SchemaRow = new CassandraTableRow(tableName, _provider, null, UsedIdentifiers, _cassandraTableMetadata);
        }

        #region ITable Members
        public bool SupportsCount { get { return _provider.SupportCount; } }

        public bool SupportsSorting { get { return true; } }
        public bool SupportsAdvancedSorting { get { return true; } }
        public bool SupportsFiltering { get { return true; } }

        public void ApplySort(string sortDescription)
        {
            _orderClause = sortDescription.Replace("\t", ",").Replace("[+]", "ASC").Replace("[-]", "DESC");
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
            string query = string.Format("select {0} from {1} {2} {3} {4};",
                (UsedIdentifiers != null && UsedIdentifiers.Count > 0) ? CassandraDataProvider.ListToSeparatedString(",", UsedIdentifiers) : "*",
                TableName,
                (!string.IsNullOrEmpty(_whereClause)) ? string.Format("where {0}", _whereClause) : string.Empty,
                (!string.IsNullOrEmpty(_orderClause)) ? string.Format("order by {0}", _orderClause) : string.Empty,
                (!string.IsNullOrEmpty(_whereClause)) ? "allow filtering" : string.Empty);

            SimpleStatement statement = new SimpleStatement(query);
            RowSet rows = new RowSet();
            try
            {
                rows = _provider.CassandraSession.Execute(statement);
            }
            catch (Exception ex)
            {
                _provider.Logger.Error(LogCategory.DataProvider, "Cassandra: Error while executing statement:\n{0}\n\nDetails:\n{1}", query, ex.ToString());
                throw;
            }
            finally
            {
                _orderClause = null;
                _whereClause = null;
            }

            foreach (var row in rows)
            {
                yield return new CassandraTableRow(TableName, _provider, row, UsedIdentifiers, _cassandraTableMetadata);
            }

        }

        public System.Collections.ObjectModel.ReadOnlyCollection<string> SortDescriptions
        {
            get
            {
                List<string> sortOrders = new List<string>();

                foreach (var column in SchemaRow.Columns)
                {
                    if ((column as CassandraTableColumn).CassandraColumnType != "clustering_key" && (column as CassandraTableColumn).CassandraColumnType != "partition_key")
                        continue;
                    // add sort order
                    sortOrders.Add(String.Concat(column.ColumnName, " [+]"));
                    sortOrders.Add(String.Concat(column.ColumnName, " [-]"));
                }

                return sortOrders.AsReadOnly();
            }
        }

        public ITableRow SchemaRow { get; private set; }

        #endregion

        #region IAdvancedFiltering Members
        public void ApplyAdvancedFilter(string filter, object[] parameters)
        {
            ApplyFilter(filter);
        }

        private static string EscapeArgumentForRowFilter(string rowFilter)
        {
            string input = rowFilter.Substring(1, rowFilter.Length - 2);
            StringBuilder result = new StringBuilder();

            foreach (char c in input)
            {
                switch (c)
                {
                    case '[':
                    case ']':
                    case '*':
                    case '%':
                        result.Append(String.Format("{0}", c));
                        break;
                    default:
                        result.Append(c);
                        break;
                }
            }
            return result.ToString();
        }

        public object TranslateFilterSyntax(LlExpressionPart part, ref object name, int argumentCount, object[] arguments)
        {
            switch (part)
            {
                case LlExpressionPart.Unknown:
                    {
                        return null;
                    }
                case LlExpressionPart.Boolean:
                    {
                        return null;
                    }
                case LlExpressionPart.Text:
                    {
                        if (arguments[0] != null)
                        {
                            Regex guidRegex = new Regex(@"\b[a-fA-F0-9]{8}(?:-[a-fA-F0-9]{4}){3}-[a-fA-F0-9]{12}\b");
                            Match guidMatch = guidRegex.Match(arguments[0].ToString());
                            if (guidMatch.Success)
                                return arguments[0].ToString();
                            else
                                return String.Format("'{0}'", arguments[0]);
                        }
                        else
                        {
                            return "null";
                        }
                    }
                case LlExpressionPart.Number:
                    {
                        if (arguments[0] != null)
                        {
                            return arguments[0].ToString();
                        }
                        else
                        {
                            return "null";
                        }
                    }
                case LlExpressionPart.Date:
                    {
                        if (arguments[0] != null)
                        {
                            return String.Format("'{0}'", ((DateTime)arguments[0]).ToString("yyyy-MM-dd HH:mm:ss"));
                        }
                        else
                        {
                            return "null";
                        }
                    }
                case LlExpressionPart.UnaryOperatorSign:
                    {
                        return (String.Format("-{0}", arguments[0]));
                    }
                case LlExpressionPart.UnaryOperatorNegation:
                    {
                        return null;
                    }
                case LlExpressionPart.BinaryOperatorAdd:
                    {
                        return null;
                    }
                case LlExpressionPart.BinaryOperatorSubtract:
                    {
                        return null;
                    }
                case LlExpressionPart.BinaryOperatorMultiply:
                    {
                        return null;
                    }
                case LlExpressionPart.BinaryOperatorDivide:
                    {
                        return null;
                    }
                case LlExpressionPart.BinaryOperatorModulo:
                    {
                        return null;
                    }
                case LlExpressionPart.RelationXor:
                    {
                        return null;
                    }
                case LlExpressionPart.RelationOr:
                    {
                        return null;
                    }
                case LlExpressionPart.RelationAnd:
                    {
                        return (String.Format("({0} AND {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationEqual:
                    {
                        if (SchemaRow.Columns.Any(c => c.ColumnName == arguments[0].ToString() && (c as CassandraTableColumn).CassandraColumnType == "regular") ||
                            (SchemaRow.Columns.Any(c => c.ColumnName == arguments[0].ToString() && (c as CassandraTableColumn).CassandraColumnType == "clustering_key")
                             && SchemaRow.Columns.Where(c => (c as CassandraTableColumn).CassandraColumnType == "clustering_key").Count() > 1))
                            return null;
                        if (!name.ToString().Equals("@ARG:MULTIVALUE", StringComparison.Ordinal))
                        {
                            return (String.Format("({0} = {1})", arguments[0], arguments[1]));
                        }
                        else
                        {
                            if ((string)arguments[1] != String.Empty)
                                return (String.Format("({0} IN ({1}))", arguments[0], arguments[1]));
                            else
                                return "(1=0)";
                        }
                    }
                case LlExpressionPart.RelationNotEqual:
                    {
                        if (SchemaRow.Columns.Any(c => c.ColumnName == arguments[0].ToString() && (c as CassandraTableColumn).CassandraColumnType == "regular") ||
                            (SchemaRow.Columns.Any(c => c.ColumnName == arguments[0].ToString() && (c as CassandraTableColumn).CassandraColumnType == "clustering_key")
                             && SchemaRow.Columns.Where(c => (c as CassandraTableColumn).CassandraColumnType == "clustering_key").Count() > 1))
                            return null;
                        if (!name.ToString().Equals("@ARG:MULTIVALUE", StringComparison.Ordinal))
                        {
                            return (String.Format("({0} <> {1})", arguments[0], arguments[1]));
                        }
                        else
                        {
                            if ((string)arguments[1] != String.Empty)
                                return (String.Format("(NOT ({0} IN ({1})))", arguments[0], arguments[1]));
                            else
                                return "(1=1)";
                        }
                    }
                case LlExpressionPart.RelationGreaterThan:
                    {
                        return (String.Format("({0} > {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationGreaterThanOrEqual:
                    {
                        return (String.Format("({0} >= {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationLessThan:
                    {
                        return (String.Format("({0} < {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationLessThanOrEqual:
                    {
                        return (String.Format("({0} <= {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.Function:
                    {
                        switch (name.ToString().ToUpper())
                        {
                            case "":
                                return "";
                            case "EMPTY":
                                return (String.Format("{0}=''", arguments[0]));
                        }
                        return null;
                    }
                case LlExpressionPart.Field:
                    {
                        if (arguments[0] == null)
                            return null;

                        string fieldName = arguments[0].ToString();
                        if (!fieldName.StartsWith(TableName + ".") || fieldName.Contains("@"))
                        {
                            return null;
                        }

                        return String.Format("{0}", fieldName.Substring(TableName.Length + 1));
                    }
                default:
                    return null;
            }
        }
        #endregion

        #region ICanHandleUsedIdentifiers
        public void SetUsedIdentifiers(ReadOnlyCollection<string> identifiers)
        {
            UsedIdentifiers = identifiers;
        }
        #endregion
    }

    internal class CassandraTableRow : ITableRow
    {
        private List<ITableColumn> _columns;
        private CassandraDataProvider _provider;
        private Row _data;
        private ReadOnlyCollection<string> _usedIdentifiers;
        private TableMetadata CassandraTableMetadata;

        public CassandraTableRow(string tableName, CassandraDataProvider provider, Row data, ReadOnlyCollection<string> usedIdentifiers, TableMetadata tableMetadata)
        {
            TableName = tableName;
            _provider = provider;
            _data = data;
            _usedIdentifiers = usedIdentifiers;
            CassandraTableMetadata = tableMetadata;
        }

        private void InitColumns()
        {
            _columns = new List<ITableColumn>();
            RowSet rows = _provider.CassandraSession.Execute(string.Format("select * from system.schema_columns where columnfamily_name = '{0}' allow filtering;", TableName));

            foreach (var row in rows)
            {
                if (_usedIdentifiers == null || _usedIdentifiers.Contains(row["column_name"].ToString()))
                {
                    TableColumn column = CassandraTableMetadata.TableColumns.Where(c => c.Name == row["column_name"].ToString()).First();
                    Type dataType = GetDataType(column.TypeCode);
                    if (_data != null)
                        _columns.Add(new CassandraTableColumn(row["column_name"].ToString(), dataType, _data[row["column_name"].ToString()], row["type"].ToString()));
                    else
                        _columns.Add(new CassandraTableColumn(row["column_name"].ToString(), dataType, GetDefaultValueForType(dataType.ToString(), "Sample Text"), row["type"].ToString()));
                    ColumnDesc cd = new ColumnDesc();
                }
            }

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
            throw new NotImplementedException();
        }

        public ITableRow GetParentRow(ITableRelation relation)
        {
            throw new NotImplementedException();
        }

        private object GetDefaultValueForType(string dataType, string stringDefaultValue)
        {
            switch (dataType)
            {
                case "System.Collections.Generic.IDictionary`2[System.String,System.String]": return new SortedDictionary<string, string> { { stringDefaultValue, stringDefaultValue } };
                case "System.Collections.Generic.IEnumerable`1[T]": return new string[] { stringDefaultValue, stringDefaultValue, stringDefaultValue };
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

        private Type GetDataType(ColumnTypeCode columnType)
        {
            switch (columnType)
            {
                case ColumnTypeCode.Ascii:
                    return typeof(string);
                case ColumnTypeCode.Bigint:
                    return typeof(long);
                case ColumnTypeCode.Blob:
                    return typeof(byte[]);
                case ColumnTypeCode.Boolean:
                    return typeof(bool);
                case ColumnTypeCode.Counter:
                    return typeof(long);
                case ColumnTypeCode.Custom:
                    return typeof(string);
                case ColumnTypeCode.Decimal:
                    return typeof(float);
                case ColumnTypeCode.Double:
                    return typeof(double);
                case ColumnTypeCode.Float:
                    return typeof(float);
                case ColumnTypeCode.Inet:
                    return typeof(string);
                case ColumnTypeCode.Int:
                    return typeof(int);
                case ColumnTypeCode.List:
                    return typeof(IEnumerable<>);
                case ColumnTypeCode.Map:
                    return typeof(IDictionary<string, string>);
                case ColumnTypeCode.Set:
                    return typeof(IEnumerable<>);
                case ColumnTypeCode.Text:
                    return typeof(string);
                case ColumnTypeCode.Timestamp:
                    return typeof(DateTimeOffset);
                case ColumnTypeCode.Timeuuid:
                    return typeof(Guid);
                case ColumnTypeCode.Tuple:
                    return typeof(IEnumerable<>);
                case ColumnTypeCode.Udt:
                    return typeof(DateTimeOffset);
                case ColumnTypeCode.Uuid:
                    return typeof(Guid);
                case ColumnTypeCode.Varchar:
                    return typeof(string);
                case ColumnTypeCode.Varint:
                    return typeof(BigInteger);

                default:
                    return null;
            }

        }

    }

    internal class CassandraTableColumn : ITableColumn
    {
        public string ColumnName { get; private set; }
        public Type DataType { get; private set; }
        public object Content { get; private set; }
        public LlFieldType FieldType { get { return LlFieldType.Unknown; } }
        public string CassandraColumnType { get; private set; }

        public CassandraTableColumn(string columnName, Type dataType, object content, string type)
        {
            ColumnName = columnName;
            DataType = dataType;
            Content = content;
            CassandraColumnType = type;


            switch (dataType.Name)
            {
                case "IDictionary`2":
                    DataType = typeof(string);                    
                    Content = CassandraDataProvider.DictionaryToSeparatedPairs(",", content as IDictionary<string, string>);
                    break;
                case "IEnumerable`1":
                    DataType = typeof(string);
                    Content = CassandraDataProvider.ListToSeparatedString(",", content as IList<string> != null ? content as IList<string> : content as string[]);
                    break;
                default:
                    break;
            }
        }
    }
}
