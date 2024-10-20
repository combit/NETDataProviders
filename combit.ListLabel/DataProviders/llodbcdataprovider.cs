using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Odbc;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace combit.Reporting.DataProviders
{
    [Serializable]
    public sealed class OdbcConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {

        public OdbcConnectionDataProvider(OdbcConnection connection)
            : this(connection, "[{0}]")
        {
        }

        public OdbcConnectionDataProvider(OdbcConnection connection, string identifierDelimiterFormat, string parameterMarkerFormat = null)
        {
            Connection = (Provider.CloneConnection(connection));
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            IdentifierDelimiterFormat = identifierDelimiterFormat;
            ParameterMarkerFormat = parameterMarkerFormat;
            SupportsAdvancedFiltering = false;
            PrefixTableNameWithSchema = false;
        }

        private OdbcConnectionDataProvider()
            : base()
        {
        }

        private OdbcConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("OdbcConnectionDataProvider.Version");
            if (version >= 1)
            {
                Connection = new OdbcConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
            }
            if (version >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
            if (version >= 3)
            {
                PrefixTableNameWithSchema = info.GetBoolean("PrefixTableNameWithSchema");
            }
            if (version >= 4)
            {
                IdentifierDelimiterFormat = info.GetString("IdentifierDelimiterFormat");
            }
            if (version >= 5)
            {
                ParameterMarkerFormat = info.GetString("ParameterMarkerFormat");
            }
        }

        public override bool SupportsAdvancedFiltering { get; set; }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public bool PrefixTableNameWithSchema { get; set; }

        public string IdentifierDelimiterFormat { get; set; }
        public string ParameterMarkerFormat { get; set; }

        /// <summary>Option to skip (slow) analysis of table relations if only the available tables are needed.</summary>
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public bool DisableRelations { get; set; }   // used for datasource configuration in Report Server

        // An event that clients can use to add relations to data provider
        public event EventHandler<AddRelationEventArgs> AddRelations;

        // Invoke the AddRelations event;
        private void OnAddRelations(AddRelationEventArgs e)
        {
            if (AddRelations != null)
                AddRelations(this, e);
        }

        protected override void Init()
        {
            if (Initialized)
                return;

            Connection.Open();
            try
            {
                OdbcConnection odbcConnection = Connection as OdbcConnection;

                Dictionary<string, bool> passedTableNames = new Dictionary<string, bool>();  // we don`t care about the values, but still use dictionaries to have fast lookups of existing keys
                Dictionary<string, bool> passedRelationNames = new Dictionary<string, bool>();
                Provider.PrefixTableNameWithSchema = PrefixTableNameWithSchema;

                if ((SupportedElementTypes & DbConnectionElementTypes.Table) != 0)
                {
                    DbCommandSetDataProviderHelper.SafeOpen(odbcConnection);
                    DataTable tables = odbcConnection.GetSchema("Tables");
                    DataProviderHelper.LogDataTableStructure(Logger, tables);

                    foreach (DataRow dr in tables.Rows)
                    {
                        string tableSchema = null;

                        if (tables.Columns.Contains("Table_Type"))
                        {
                            // Excel passes data as system table
                            if (!dr["Table_Type"].ToString().Equals("TABLE", StringComparison.OrdinalIgnoreCase) && !odbcConnection.DataSource.Equals("EXCEL", StringComparison.OrdinalIgnoreCase))
                                continue;

                            if (tables.Columns.Contains("Table_Schem"))
                            {
                                tableSchema = dr["Table_Schem"].ToString();

                                if (tableSchema.Equals("SYS", StringComparison.OrdinalIgnoreCase)
                                    || tableSchema.Equals("ml_server", StringComparison.OrdinalIgnoreCase))
                                    continue;
                            }
                        }

                        if (tables.Columns.Contains("TYPENAME") && dr["TYPENAME"].ToString() != "TABLE")
                        {
                            continue;
                        }

                        string tableName = string.Empty;
                        if (tables.Columns.Contains("Table_Name"))
                        {
                            tableName = dr["Table_Name"].ToString();
                        }
                        else if (tables.Columns.Contains("TableName"))
                        {
                            tableName = dr["TableName"].ToString();
                        }
                        else
                        {
                            throw new ListLabelException("OdbcDataProvider has no TableName column in schema information. Connection cannot be used.");
                        }

                        if (SuppressAddTableOrRelation(tableName, tableSchema))
                            continue;

                        String select = String.Empty;

                        if (PrefixTableNameWithSchema && !String.IsNullOrEmpty(tableSchema))
                        {
                            select = "SELECT * FROM " + String.Format(IdentifierDelimiterFormat, tableSchema) + "." + String.Format(IdentifierDelimiterFormat, tableName);
                            passedTableNames.Add(tableSchema + "." + tableName, true);
                        }
                        else
                        {
                            select = String.Format("SELECT * FROM " + IdentifierDelimiterFormat, tableName);
                            passedTableNames.Add(tableName, true);
                        }
                        OdbcCommand command = new OdbcCommand(select, odbcConnection);
                        AddCommand(command, tableName, IdentifierDelimiterFormat, ParameterMarkerFormat);
                    }
                }

                if ((SupportedElementTypes & DbConnectionElementTypes.View) != 0)
                {
                    DbCommandSetDataProviderHelper.SafeOpen(odbcConnection);
                    DataTable views = odbcConnection.GetSchema("Views");
                    DataProviderHelper.LogDataTableStructure(Logger, views);

                    foreach (DataRow dr in views.Rows)
                    {
                        string viewSchema = null;

                        if (views.Columns.Contains("Table_Type"))
                        {
                            if (dr["Table_Type"].ToString() != "VIEW")
                                continue;

                            if (views.Columns.Contains("Table_Schem"))
                            {
                                viewSchema = dr["Table_Schem"].ToString();

                                // remove sys and information_schema views...
                                if (viewSchema.Equals("SYS", StringComparison.OrdinalIgnoreCase)
                                     || viewSchema.Equals("ml_server", StringComparison.OrdinalIgnoreCase))
                                    continue;
                                if (viewSchema.Equals("INFORMATION_SCHEMA", StringComparison.OrdinalIgnoreCase))
                                    continue;
                            }
                        }

                        if (views.Columns.Contains("TYPENAME") && dr["TYPENAME"].ToString() != "VIEW")
                        {
                            continue;
                        }

                        string tableName = string.Empty;
                        if (views.Columns.Contains("Table_Name"))
                        {
                            tableName = dr["Table_Name"].ToString();
                        }
                        else if (views.Columns.Contains("TableName"))
                        {
                            tableName = dr["TableName"].ToString();
                        }
                        else
                        {
                            throw new ListLabelException("ODBC data provider has not TableName column in schema information.");
                        }

                        if (SuppressAddTableOrRelation(tableName, viewSchema))
                            continue;

                        String select = String.Empty;
                        if (PrefixTableNameWithSchema && !String.IsNullOrEmpty(viewSchema))
                        {
                            select = "SELECT * FROM " + String.Format(IdentifierDelimiterFormat, viewSchema) + "." + String.Format(IdentifierDelimiterFormat, tableName);
                            passedTableNames.Add(viewSchema + "." + tableName, true);
                        }
                        else
                        {
                            select = String.Format("SELECT * FROM " + IdentifierDelimiterFormat, tableName);
                            passedTableNames.Add(tableName, true);
                        }

                        OdbcCommand command = new OdbcCommand(select, odbcConnection);
                        AddCommand(command, tableName, IdentifierDelimiterFormat, ParameterMarkerFormat);
                    }
                }

                // relations
                if (!DisableRelations)
                {
                    OnAddRelations(new AddRelationEventArgs() { Provider = base.Provider });

                    try
                    {
                        string commandText = "SELECT b.table_name AS PrimaryTable, b.column_name AS PrimaryField, a.table_name AS ForeignTable, a.column_name AS ForeignField ";

                        if (PrefixTableNameWithSchema)
                        {
                            commandText += ", b.table_schema AS PrimarySchema, a.table_schema as ForeignSchema ";
                        }

                        commandText += @"
                        FROM information_schema.referential_constraints 
                        LEFT JOIN information_schema.key_column_usage AS a ON referential_constraints.constraint_name = a.constraint_name 
                        LEFT JOIN information_schema.key_column_usage AS b ON referential_constraints.unique_constraint_name = b.constraint_name";

                        DbCommandSetDataProviderHelper.SafeOpen(odbcConnection);
                        OdbcCommand relationCommand = new OdbcCommand(commandText, odbcConnection);
                        OdbcDataReader reader = relationCommand.ExecuteReader(CommandBehavior.CloseConnection);
                        while (reader.Read())
                        {
                            string parentTableName = reader["PrimaryTable"].ToString().Trim();
                            string parentColumnName = reader["PrimaryField"].ToString().Trim();
                            string childTableName = reader["ForeignTable"].ToString().Trim();
                            string childColumnName = reader["ForeignField"].ToString().Trim();
                            string parentSchema = null;
                            string childSchema = null;
                            if (PrefixTableNameWithSchema)
                            {
                                parentSchema = reader["PrimarySchema"].ToString().Trim(); ;
                                childSchema = reader["ForeignSchema"].ToString().Trim(); ;
                            }

                            // check if both tables of the relation have been added at all
                            if (PrefixTableNameWithSchema)
                            {
                                if (!passedTableNames.ContainsKey(parentSchema + "." + parentTableName) || !passedTableNames.ContainsKey(childSchema + "." + childTableName))
                                {
                                    continue;
                                }
                            }
                            else
                            {
                                if (!passedTableNames.ContainsKey(parentTableName) || !passedTableNames.ContainsKey(childTableName))
                                {
                                    continue;
                                }
                            }


                            if (SuppressAddTableOrRelation(parentTableName, parentSchema) || SuppressAddTableOrRelation(childTableName, childSchema))
                                continue;

                            string relName = parentTableName + "2" + childTableName;
                            int relationIndex = 1;
                            string formatString = relName + "{0}";

                            while (passedRelationNames.ContainsKey(relName))
                            {
                                relName = String.Format(System.Globalization.CultureInfo.InvariantCulture, formatString, relationIndex);
                                relationIndex++;
                            }
                            passedRelationNames.Add(relName, true);

                            if (PrefixTableNameWithSchema)
                                AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName, parentSchema, childSchema);
                            else
                                AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName);

                        }
                        reader.Close();
                    }
                    catch (Exception e)
                    {
                        Logger.Error(LogCategory.DataProvider, "Exception while adding relation: " + e.ToString());
                    }
                }
            }
            finally
            {
                Connection.Close();
                Initialized = true;
            }
        }

        public delegate void AddRelationsEventHandler(object sender, AddRelationEventArgs e);

        public class AddRelationEventArgs : EventArgs
        {
            public DbCommandSetDataProvider Provider { get; set; }
        }


        #region ISerializable Members
#if !NET_BUILD
        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
#endif
        void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            // v1:
            info.AddValue("OdbcConnectionDataProvider.Version", 5);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            // v2:
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
            // v3:
            info.AddValue("PrefixTableNameWithSchema", PrefixTableNameWithSchema);
            // v4:
            info.AddValue("IdentifierDelimiterFormat", IdentifierDelimiterFormat);
            // v5:
            info.AddValue("ParameterMarkerFormat", ParameterMarkerFormat);
        }

#endregion
    }

    public class AddRelationEventArgs : EventArgs
    {
        public DbCommandSetDataProvider Provider { get; set; }
    }

}