using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;
using MySqlConnector;

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provider for MySql, see https://mysqlconnector.net/
    /// </summary>
    [Serializable]
    public class MySqlConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {
        protected MySqlConnectionDataProvider()
        {
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            Provider.CancelBeforeClose = false;
            SupportsAdvancedFiltering = true;
            PrefixTableNameWithSchema = false;
        }

        protected MySqlConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("MySqlConnectionDataProvider.Version");
            if (version >= 1)
            {
                Connection = CreateDbConnection(info.GetString("ConnectionString"));
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
            }
            if (version >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
            if (version >= 3)
            {
                PrefixTableNameWithSchema = info.GetBoolean("PrefixTableNameWithSchema");
            }
        }

        public MySqlConnectionDataProvider(IDbConnection connection)
            :this()
        {
            if (!(connection is MySqlConnection))
                throw new ListLabelException("The connection object must be a valid MySQL connection");
            Connection = connection;
        }


        public MySqlConnectionDataProvider(string connectionString)
            :this()
        {
            Connection = CreateDbConnection(connectionString);
        }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public bool PrefixTableNameWithSchema { get; set; }

        public override bool SupportsAdvancedFiltering { get; set; }

        private static DbConnection CreateDbConnection(string connectionString)
        {
            // Assume failure.
            DbConnection connection = null;

            // Create the DbConnection.
            if (connectionString != null)
            {
                try
                {
                    connection = new MySqlConnection(connectionString);                    
                }
                catch (Exception ex)
                {
                    // Set the connection to null if it was created.
                    if (connection != null)
                    {
                        connection = null;
                    }
                    LoggingHelper.LogExceptionDetails(ex);
                    throw;
                }
            }
            // Return the connection.
            return connection;
        }

        protected override void Init()
        {
            if (Initialized)
                return;

            HashSet<String> passedRelationNames = new HashSet<string>();
            HashSet<String> passedSchemas = new HashSet<string>();
            HashSet<String> passedTables = new HashSet<string>();
            HashSet<String> excludedOwners = new HashSet<string>();
            excludedOwners.Add("mysql");
            excludedOwners.Add("information_schema");
            excludedOwners.Add("sys");
            Provider.PrefixTableNameWithSchema = PrefixTableNameWithSchema;

            if (Connection.State != ConnectionState.Open)
                Connection.Open();

            DbCommand cmd;
            try
            {
                DataTable views = null;
                DataTable tables = null;
                if (SupportedElementTypes.HasFlag(DbConnectionElementTypes.Table))
                {
                    tables = (Connection as DbConnection).GetSchema("Tables");
                    DataProviderHelper.LogDataTableStructure(Logger, tables);
                }
                if (SupportedElementTypes.HasFlag(DbConnectionElementTypes.View))
                {
                    views = (Connection as DbConnection).GetSchema("Views");
                    DataProviderHelper.LogDataTableStructure(Logger, views);
                }
                Connection.Close();

                for (int pass = 0; pass < 2; pass++)
                {
                    DataTable currentTable;
                    switch (pass)
                    {
                        case 0:
                            currentTable = views;
                            break;
                        case 1:
                            currentTable = tables;
                            break;
                        default:
                            currentTable = null;
                            break;
                    }

                    if (currentTable == null)
                        continue;

                    foreach (DataRow dr in currentTable.Rows)
                    {
                        string schema = dr["TABLE_SCHEMA"].ToString();
                        string name = dr["TABLE_NAME"].ToString();
                        string fullName = $"{schema}.{name}";
                        string tableType = pass == 1 ? dr["TABLE_TYPE"].ToString() : String.Empty;

                        // the views are included in the tables, depending on the driver version
                        if (pass == 1 && 
                            tableType == "VIEW" && 
                            (!SupportedElementTypes.HasFlag(DbConnectionElementTypes.View) 
                            || passedTables.Contains(fullName)))
                            continue;

                        if (SuppressAddTableOrRelation(name, schema))
                            continue;

                        if (excludedOwners.Contains(schema))
                            continue;

                        // Get db from connection string
                        DbConnection connection = (DbConnection)Connection;
                        string database = connection.Database;

                        // If no database is specified in the connectionString, get all tables of all schemas,
                        // otherwise just get the tables of the specified db
                        if ((schema != database) && !String.IsNullOrEmpty(database))
                            continue;

                        // No schema specified, add them to list, to build relations
                        if (!passedSchemas.Contains(schema))
                        {
                            passedSchemas.Add(schema);
                        }

                        ICloneable cloneable = (ICloneable)Connection;
                        DbConnection newConnection = (DbConnection)cloneable.Clone();
                        cmd = newConnection.CreateCommand();
                        cmd.CommandText = "Select * From " + (String.IsNullOrEmpty(schema) ? "`" + name + "`" : "`" + schema + "`.`" + name + "`") + "";
                        AddCommand(cmd, name, "`{0}`", "?{0}");
                        passedTables.Add(fullName);
                    }
                }

                foreach (string schema in passedSchemas)
                {
                    string commandText = String.Format(CultureInfo.InvariantCulture, "SELECT K.`TABLE_NAME`, K.`COLUMN_NAME`, K.`REFERENCED_TABLE_NAME`, K.`REFERENCED_COLUMN_NAME`, K.`CONSTRAINT_NAME` FROM information_schema.KEY_COLUMN_USAGE K WHERE `TABLE_SCHEMA`='{0}' ORDER BY K.`CONSTRAINT_NAME`, K.`POSITION_IN_UNIQUE_CONSTRAINT`", schema);
                    cmd = (Connection as DbConnection).CreateCommand();
                    cmd.CommandText = commandText;
                    using (cmd)
                    {
                        Connection.Open();
                        DbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                        string lastConstraintName = string.Empty;

                        while (reader.Read())
                        {
                            if (!reader.IsDBNull(2) && !reader.IsDBNull(3))
                            {
                                string childColumnName = reader.GetString(1);
                                string parentColumnName = reader.GetString(3);
                                string childTableName = reader.GetString(0);
                                string parentTableName = reader.GetString(2);
                                string constraintName = reader.GetString(4);
                                if (SuppressAddTableOrRelation(parentTableName, null) || SuppressAddTableOrRelation(childTableName, null))
                                    continue;

                                if (lastConstraintName == constraintName)
                                {
                                    IDataProvider providerInterface = (Provider as IDataProvider);
                                    DbCommandTableRelation lastRelation = providerInterface.Relations[providerInterface.Relations.Count - 1] as DbCommandTableRelation;
                                    lastRelation.ChildColumnName += '\t' + childColumnName;
                                    lastRelation.ParentColumnName += '\t' + parentColumnName;
                                    continue;
                                }
                                lastConstraintName = constraintName;

                                string relName = parentTableName + "2" + childTableName;
                                int relationIndex = 1;
                                string formatString = relName + "{0}";

                                while (passedRelationNames.Contains(relName))
                                {
                                    relName = String.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
                                    relationIndex++;
                                }
                                passedRelationNames.Add(relName);
                                AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName, PrefixTableNameWithSchema ? schema : null, PrefixTableNameWithSchema ? schema : null);
                            }
                        }
                        reader.Close();
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
                case LlExpressionPart.BinaryOperatorAdd:
                    {
                        if (e.Arguments[0] is String && e.Arguments[1] is String)
                        {
                            e.Result = String.Format("CONCAT({0},{1})", e.Arguments[0], e.Arguments[1]);
                            e.Handled = true;
                        }
                    }
                    break;
                case LlExpressionPart.Function:
                    {
                        switch (e.Name.ToString().ToUpper())
                        {
                            case "STARTSWITH":
                                e.Result = String.Format("({0} LIKE CONCAT({1},'%'))", e.Arguments[0], e.Arguments[1]);
                                e.Handled = true;
                                break;
                            case "ENDSWITH":
                                e.Result = String.Format("({0} LIKE CONCAT('%',{1}))", e.Arguments[0], e.Arguments[1]);
                                e.Handled = true;
                                break;
                            case "CONTAINS":
                                e.Result = String.Format("({0} LIKE CONCAT('%', CONCAT({1},'%')))", e.Arguments[0], e.Arguments[1]);
                                e.Handled = true;
                                break;
                            case "LEN":
                                e.Result = String.Format("(LENGTH({0}))", e.Arguments[0]);
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
                    }
                default:
                    break;
            }
        }

        //http://dev.mysql.com/doc/refman/5.0/en/group-by-functions.html
        protected override string GetNativeAggregateFunctionName(NativeAggregateFunction function)
        {
            switch (function)
            {
                case NativeAggregateFunction.StdDevSamp:
                    return "STDDEV_SAMP";
                case NativeAggregateFunction.StdDevPop:
                    return "STDDEV";
                case NativeAggregateFunction.VarSamp:
                    return "VARIANCE_SAMP";
                case NativeAggregateFunction.VarPop:
                    return "VARIANCE";
                default:
                    return function.ToString().ToUpperInvariant();
            }
        }

        #region ISerializable Members

#if !NET_BUILD
        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
#endif
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("MySqlConnectionDataProvider.Version", 3);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
            info.AddValue("PrefixTableNameWithSchema", PrefixTableNameWithSchema);
        }

#endregion
    }
    [Serializable]
    public class MariaDBConnectionDataProvider : MySqlConnectionDataProvider
    {
        protected MariaDBConnectionDataProvider()
            : base()
        {
        }

        protected MariaDBConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        public MariaDBConnectionDataProvider(IDbConnection connection)
            : base(connection)
        {
        }

        public MariaDBConnectionDataProvider(string connectionString)
            : base(connectionString)
        {
        }
    }
}

