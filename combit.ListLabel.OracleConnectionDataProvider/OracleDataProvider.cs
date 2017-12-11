using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Reflection;
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Text;

namespace combit.ListLabel23.DataProviders
{
    /// <summary>
    /// Replaces both the old data provider for System.Data.Oracle.OracleConnection and the provider
    /// from combit.ListLabel20.OracleConnectionDataProvider.dll with a new provider that uses the
    /// ADO.NET providers Oracle.ManagedDataAccess.Client (preferred) or Oracle.DataAccess.Client (fallback).
    /// ODP.NET has to be installed to use this data provider.
    /// </summary>
    [Serializable]
    public sealed class OracleConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {

        private readonly ReadOnlyCollection<String> _tableOwners;
        private readonly List<string> _excludedOwners;

        public bool PrefixTableNameWithOwner { get; set; }
        public override bool SupportsAdvancedFiltering { get; set; }

        /// <summary>Option to skip (slow) analysis of table relations if only the available tables are needed.</summary>
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public bool DisableRelations { get; set; }   // used for datasource configuration in Report Server

        private OracleConnectionDataProvider()
            : base()
        {
            SupportsAdvancedFiltering = true;
            SupportedElementTypes = DbConnectionElementTypes.Table;

            // list from http://docs.oracle.com/cd/B28359_01/server.111/b28337/tdpsg_user_accounts.htm#TDPSG20302
            var excludedOwners = new string[] {"CTXSYS", "DBSNMP", "EXFSYS", "FLOWS_FILES", "IX", "LBACSYS", "MDSYS", "MGMT_VIEW",
                                                "OLAPSYS", "ORDPLUGINS", "ORDSYS", "OUTLN",
                                                "SI_INFORMTN_SCHEMA", "SYS", "SYSMAN", "SYSTEM", "TSMSYS", "WK_TEST",
                                                "WKSYS", "WKPROXY", "WMSYS", "XDB"};

            _excludedOwners = new List<string>(excludedOwners.Length);
            _excludedOwners.AddRange(excludedOwners);
        }

        public OracleConnectionDataProvider(string connectionString)
            : this(connectionString, String.Empty)
        { }

        public OracleConnectionDataProvider(string connectionString, string tableOwner)
            : this()
        {
            Connection = CreateDbConnection(connectionString);
            List<string> owners = new List<string>(1);
            if (!String.IsNullOrEmpty(tableOwner))
                owners.Add(tableOwner);
            _tableOwners = owners.AsReadOnly();

        }

        public OracleConnectionDataProvider(string connectionString, ReadOnlyCollection<string> tableOwners)
            : this()
        {
            Connection = CreateDbConnection(connectionString);
            _tableOwners = tableOwners;
        }


        // gets called by XmlSerializer
        private OracleConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            if (info.GetInt32("OracleConnectionDataProvider.Version") >= 1)
            {
                Connection = CreateDbConnection(info.GetString("ConnectionString"));
                Connection.ConnectionString = info.GetString("ConnectionString");
                PrefixTableNameWithOwner = info.GetBoolean("PrefixTableNameWithOwner");
                _tableOwners = (ReadOnlyCollection<string>)info.GetValue("TableOwners", typeof(ReadOnlyCollection<string>));
            }
            if (info.GetInt32("OracleConnectionDataProvider.Version") >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
        }

        /// <summary>
        /// Returns a factory of Oracle.ManagedDataAccess.Client and tries a fallback to Oracle.DataAccessClient if not available.
        /// </summary>
        /// <exception cref="ListLabelDbProviderNotInstalledException"></exception>
        public static DbProviderFactory GetOracleConnectionFactory()   // fallback is also used by Report Server when it creates connections -> public static
        {
            const string providerName = "Oracle.ManagedDataAccess.Client";
            const string providerNameFallback = "Oracle.DataAccess.Client";  // old unmanaged driver

            try
            {
                return DbProviderFactories.GetFactory(providerName);
            }
            catch (ArgumentException) { } // when not found -> silent fallback

            try
            {
                return DbProviderFactories.GetFactory(providerNameFallback);
            }
            catch (ArgumentException)
            {
                throw new ListLabelDbProviderNotInstalledException("Oracle.ManagedDataAccess.Client / Oracle.DataAccess.Client");
            }
        }

        /// <summary>
        /// Implements special handling to get values from fields of an Oracle database that cannot be mapped directly to the standard .NET data types.
        /// </summary>
        internal static object ReadSpecialValueFromRow(IDataReader reader, int columnIndex, string tableName)
        {
            Type readerType = reader.GetType();
            Type fieldType = reader.GetFieldType(columnIndex);

            if (readerType.Name != "OracleDataReader")
                throw new NotImplementedException("OracleConnectionDataProvider.ReadSpecialValueFromRow() was called for an IDataReader that is not an OracleDataReader!");

            // Oracle: reader.GetValue[i] might throw InvalidCastException or OverflowException because Oracle "NUMBER" fields can have more precision than System.Decimal supports (#16365).
            // GetDouble() did work for ODAC version 4.121.2.0, but not for the customer of #16365 -> use the Oracle datatypes with reflection
            if (fieldType == typeof(Decimal))
            {
                // First try the fast GetDouble() that seems to work at least for some values / ODP versions
                try
                {
                    return reader.GetDouble(columnIndex);
                }
                catch (Exception e)
                {
                    if (e is OverflowException || e is InvalidCastException)  // OverflowException is thrown by older ODP versions, InvalidCastException by newer
                        LlCore.LlDebugOutput("OracleConnectionDataProvider: " + e.GetType().Name + " while reading decimal value from table '" + tableName + "'. Fallback to reflection based conversion.");
                    else
                        throw;
                }

                // Fallback: Call reader.GetOracleDecimal().GetDouble() using reflection as GetOracleDecimal is not known to LL at compile time.
                // The results of GetType() and GetMethod() should be cached (takes half of the time of this method call)

                // Get OracleDecimal value
                if (_oracleDataReader_GetOracleDecimalMethod == null)
                {
                    _oracleDataReader_GetOracleDecimalMethod = readerType.GetMethod("GetOracleDecimal", new Type[] { typeof(int) });
                }
                object oracleDecimal = _oracleDataReader_GetOracleDecimalMethod.Invoke(reader, new object[] { columnIndex });

                if (oracleDecimal == DBNull.Value)
                    return null;

                // Convert to double
                if (_oracleDecimal_ToDoubleMethod == null)
                {
                    _oracleDecimal_ToDoubleMethod = oracleDecimal.GetType().GetMethod("ToDouble");
                }
                try
                {
                    return _oracleDecimal_ToDoubleMethod.Invoke(oracleDecimal, new object[0]);
                }
                catch (Exception e)
                {
                    throw new LL_BadDatabaseStructure_Exception(string.Format("OracleConnectionDataProvider: Error while reading value '{0}' from table '{1}':\n{2}", oracleDecimal.ToString(), tableName, e.ToString()));
                }
            }

            throw new NotImplementedException("OracleConnectionDataProvider.ReadSpecialValueFromRow() was called for an unknown field type: " + fieldType.FullName);
        }

        private static MethodInfo _oracleDataReader_GetOracleDecimalMethod = null;
        private static MethodInfo _oracleDecimal_ToDoubleMethod = null;


        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        private DbConnection CreateDbConnection(string connectionString)
        {
            // Assume failure
            DbConnection connection = null;

            // Create the DbProviderFactory and DbConnection.
            if (connectionString != null)
            {
                connection = GetOracleConnectionFactory().CreateConnection();
                connection.ConnectionString = connectionString;
            }
            // Return the connection.
            return connection;
        }


        private sealed class DbSchemaElement
        {
            public string Owner;
            public string TableName;
        }

        private ICollection<DbSchemaElement> GetTablesOrViews(IDbConnection connection, bool getViewsMode)
        {
            if (connection.State != ConnectionState.Open)
                throw new LL_InvalidOperation_Exception("Cannot get tables from closed connection.");

            var result = new LinkedList<DbSchemaElement>();

            string metadataTable = "ALL_TABLES";
            string tableNameColumn = "TABLE_NAME";
            if (getViewsMode)
            {
                metadataTable = "ALL_VIEWS";
                tableNameColumn = "VIEW_NAME";
            }
            var getTablesQuery = new StringBuilder();
            getTablesQuery.Append("SELECT owner, ").AppendLine(tableNameColumn).Append("FROM ").AppendLine(metadataTable);

            using (var getTablesCmd = connection.CreateCommand())
            {
                // if user has not specified to only load tables of certain users, 
                // load all tables except the default and system tables of Oracle.
                if (_tableOwners == null || _tableOwners.Count == 0)
                {
                    getTablesQuery.Append(" WHERE NOT owner IN (");
                    for (int i = 0; i < _excludedOwners.Count; ++i)
                    {
                        getTablesQuery.Append("'").Append(_excludedOwners[i]).Append("'");

                        if (i != _excludedOwners.Count - 1)
                            getTablesQuery.Append(", ");
                    }
                    getTablesQuery.Append(")");
                }
                else  // or load only from the user-specified owners
                {
                    List<string> allowedOwners = new List<string>(_tableOwners.Count);
                    for (int i = 0; i < _tableOwners.Count; ++i)  // insert a SQL parameter for each table owner (Oracle binds parameters by position, not by name!!)
                    {
                        var paramName = ":owner_" + i;
                        allowedOwners.Add(paramName);
                        var param = getTablesCmd.CreateParameter();
                        param.ParameterName = paramName;
                        param.Value = _tableOwners[i];
                        getTablesCmd.Parameters.Add(param);
                    }
                    getTablesQuery
                        .Append("WHERE owner IN (")
                        .Append(String.Join(", ", allowedOwners.ToArray()))
                        .Append(")");
                }

                // Read results
                getTablesCmd.CommandText = getTablesQuery.ToString();
                using (var reader = getTablesCmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        var ownerCol = reader.GetOrdinal("owner");
                        var tableNameCol = reader.GetOrdinal(tableNameColumn);

                        result.AddLast(new DbSchemaElement()
                        {
                            Owner = reader.GetString(ownerCol),
                            TableName = reader.GetString(tableNameCol)
                        });
                    }
                }
            }

            return result;
        }


        private bool IsWantedTable(string owner, string tableName)
        {
            if (_tableOwners != null && _tableOwners.Count != 0 && !_tableOwners.Contains(owner))
                return false;

            if (_excludedOwners.Contains(owner))
                return false;

            if (tableName.StartsWith("SYS_IOT_OVER_"))  // accessing overflow tables with Oracle 11.2 results in an exception (ORA-25191)
                return false;

            if (tableName.Contains("$"))
                return false;

            if (SuppressAddTableOrRelation(tableName, owner))
                return false;

            return true;
        }

        protected override void Init()
        {
            if (Initialized)
                return;

            List<String> passedRelationNames = new List<string>();
            Provider.PrefixTableNameWithSchema = PrefixTableNameWithOwner;


            string primaryKeyQuery = @"
                SELECT cols.table_name, cols.column_name, cols.position, cons.status, cons.owner 
                FROM all_constraints cons, all_cons_columns cols 
                WHERE cols.table_name = '{0}' AND cons.constraint_type = 'P' AND cons.constraint_name = cols.constraint_name AND cons.owner = '{1}' AND cols.owner = '{1}' 
                ORDER BY cols.table_name, cols.position";

            try
            {
                DbConnection connection = Connection as DbConnection;  // only access members of DbConnection!
                connection.Open();

                var tables = GetTablesOrViews(connection, false);
                //DataTable views = (Connection as DbConnection).GetSchema("Views");
                DataTable foreignKeys = DisableRelations ? null : (Connection as DbConnection).GetSchema("ForeignKeys");
                DataTable foreignKeyColumns = DisableRelations ? null : (Connection as DbConnection).GetSchema("ForeignKeyColumns");

                if ((SupportedElementTypes & DbConnectionElementTypes.View) == DbConnectionElementTypes.View)
                {
                    var views = GetTablesOrViews(connection, true);
                    foreach (var view in views)
                    {
                        if (!IsWantedTable(view.Owner, view.TableName))
                            continue;

                        DbConnection newConnection = (((Connection) as ICloneable).Clone() as DbConnection);
                        DbCommand newCmd = newConnection.CreateCommand();
                        newCmd.CommandText = "Select * From \"" + (String.IsNullOrEmpty(view.Owner) ? view.TableName + "\"" : view.Owner + "\".\"" + view.TableName) + "\"";

                        AddCommand(newCmd, view.TableName, "\"{0}\"", null);
                    }
                }


                foreach (var table in tables)
                {
                    string tableSchema = table.Owner;
                    string parentTableName = table.TableName;

                    if (!IsWantedTable(tableSchema, table.TableName))
                        continue;

                    DbConnection newConnection = (((Connection) as ICloneable).Clone() as DbConnection);
                    DbCommand newCmd = newConnection.CreateCommand();
                    newCmd.CommandText = "Select * From \"" + (String.IsNullOrEmpty(tableSchema) ? parentTableName + "\"" : tableSchema + "\".\"" + parentTableName) + "\"";

                    AddCommand(newCmd, parentTableName, "\"{0}\"", null);

                    // pass relations
                    if (DisableRelations)
                    {
                        continue;
                    }

                    string commandText = String.Format(CultureInfo.InvariantCulture, primaryKeyQuery, parentTableName, tableSchema);


                    DbCommand cmd = (Connection as DbConnection).CreateCommand();
                    cmd.CommandText = commandText;
                    string parentColumnName = String.Empty;
                    int parentColumnsCount = 0;
                    using (DbDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            if (!String.IsNullOrEmpty(parentColumnName))
                                parentColumnName += '\t';
                            parentColumnName += reader.GetString(1);
                            parentColumnsCount++;
                        }
                    }

                    // the foreignKeys Schema table contains the assigned foreign keys. We need to find
                    // all child tables to the current table, and additionally the column names. These are
                    // hidden in the ForeignKeyColumns Schema table...
                    foreach (DataRow keyRow in foreignKeys.Rows)
                    {
                        if ((keyRow["PRIMARY_KEY_TABLE_NAME"].ToString() == parentTableName) &&
                            (keyRow["PRIMARY_KEY_OWNER"].ToString() == tableSchema))
                        {
                            // we have a hit
                            string childTableName = keyRow["FOREIGN_KEY_TABLE_NAME"].ToString();
                            string constraintName = keyRow["FOREIGN_KEY_CONSTRAINT_NAME"].ToString();
                            string childTableOwner = keyRow["FOREIGN_KEY_OWNER"].ToString();

                            if (!IsWantedTable(childTableOwner, childTableName))
                                continue;

                            // now look up the child column name
                            string childColumnName = String.Empty;
                            int childColumnsCount = 0;
                            foreach (DataRow keyColumnRow in foreignKeyColumns.Rows)
                            {
                                if (!(keyColumnRow["CONSTRAINT_NAME"].ToString() == constraintName))
                                    continue;
                                if (!(keyColumnRow["OWNER"].ToString() == tableSchema))
                                    continue;
                                if (!String.IsNullOrEmpty(childColumnName))
                                    childColumnName += '\t';
                                childColumnName += keyColumnRow["COLUMN_NAME"];
                                childColumnsCount++;
                            }

                            if (childColumnsCount != parentColumnsCount)
                            {
                                LlCore.LlDebugOutput(String.Format("WRN: Relation from '{0}' to '{1}' (over constraint {2}) was not added, because counts of joined columns for parent and child table do not match.", parentTableName, childTableName, constraintName));
                                continue;
                            }

                            string relName = parentTableName + "2" + childTableName;
                            int relationIndex = 1;
                            string formatString = relName + "{0}";

                            while (passedRelationNames.Contains(relName))
                            {
                                relName = String.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
                                relationIndex++;
                            }
                            passedRelationNames.Add(relName);
                            AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName, tableSchema, childTableOwner);
                        }
                    }
                }
            }
            finally
            {
                Connection.Close();
                Initialized = true;
            }
        }

        protected override void OnTranslateFilterSyntax(object sender, TranslateFilterSyntaxEventArgs e)
        {
            base.OnTranslateFilterSyntax(sender, e);
            if (e.Handled)
                return;

            switch (e.Part)
            {
                case LlExpressionPart.Function:
                    switch (e.Name.ToString().ToUpper())
                    {
                        case "YEAR":
                            e.Result = String.Format("(EXTRACT(YEAR from {0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "MONTH":
                            e.Result = String.Format("(EXTRACT(MONTH from {0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "DAY":
                            e.Result = String.Format("(EXTRACT(DAY from {0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "STARTSWITH":
                            e.Result = String.Format("({0} LIKE {1}||'%')", e.Arguments[0], e.Arguments[1]);
                            e.Handled = true;
                            break;
                        case "ENDSWITH":
                            e.Result = String.Format("({0} LIKE '%'||{1})", e.Arguments[0], e.Arguments[1]);
                            e.Handled = true;
                            break;
                        case "CONTAINS":
                            e.Result = String.Format("({0} LIKE '%'||{1}||'%')", e.Arguments[0], e.Arguments[1]);
                            e.Handled = true;
                            break;
                        case "MID$":
                            if (e.ArgumentCount == 2)
                                e.Result = (String.Format("(SUBSTR({0},{1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString() + "+1"));
                            else
                                e.Result = (String.Format("(SUBSTR({0},{1},{2}))", e.Arguments[0].ToString(), e.Arguments[1].ToString() + "+1", e.Arguments[2].ToString()));
                            e.Handled = true;
                            break;
                        case "LEFT$":
                            e.Result = (String.Format("(SUBSTR({0},0,{1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString()));
                            e.Handled = true;
                            break;
                        case "RIGHT$":
                            e.Result = (String.Format("(SUBSTR({0},-{1},{1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString()));
                            e.Handled = true;
                            break;
                        case "LEN":
                            e.Result = (String.Format("(LENGTH({0}))", e.Arguments[0].ToString()));
                            e.Handled = true;
                            break;
                        case "EMPTY":
                            if (e.ArgumentCount == 1)
                                e.Result = String.Format("(LENGTH({0}) = 0)", e.Arguments[0]);
                            else
                                if ((bool)e.Arguments[1])
                                    e.Result = String.Format("(LENGTH(LTRIM(RTRIM({0}))) = 0)", e.Arguments[0]);
                                else
                                    e.Result = String.Format("(LENGTH({0}) = 0)", e.Arguments[0]);
                            e.Handled = true;
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }

        #region ISerializable Members
        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("OracleConnectionDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("PrefixTableNameWithOwner", PrefixTableNameWithOwner);
            info.AddValue("TableOwners", _tableOwners);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }

        #endregion
    }
}
