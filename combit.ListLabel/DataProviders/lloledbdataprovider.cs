using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Text.RegularExpressions;

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provides a data provider for OLE DB database connections.
    /// </summary>
    /// <remarks>
    /// This class extends <see cref="DbConnectionDataProvider"/> and supports OLE DB connections,
    /// allowing interaction with database elements such as tables and views.
    /// </remarks>
    /// <example>
    /// The following example demonstrates how to use the <see cref="OleDbConnectionDataProvider"/>:
    /// <code language="csharp">
    /// // Create an OLE DB connection using your connection string.
    /// OleDbConnection connection = new OleDbConnection("your connection string");
    /// 
    /// // Create an instance of the OleDbConnectionDataProvider with a specified identifier delimiter format.
    /// OleDbConnectionDataProvider provider = new OleDbConnectionDataProvider(connection, "[{0}]");
    /// 
    /// // Assign the provider as the data source.
    /// using ListLabel listLabel = new ListLabel();
    /// listLabel.DataSource = provider;
    /// ExportConfiguration exportConfiguration = new ExportConfiguration(LlExportTarget.Pdf, exportFilePath, projectFilePath);
    /// exportConfiguration.ShowResult = true;
    /// listLabel.Export(exportConfiguration);
    /// </code>
    /// </example>
    [Serializable]
    public class OleDbConnectionDataProvider : DbConnectionDataProvider, ISerializable, IFileList
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OleDbConnectionDataProvider"/> class.
        /// </summary>
        public OleDbConnectionDataProvider()
            : base()
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="OleDbConnectionDataProvider"/> class
        /// using the specified OLE DB connection.
        /// </summary>
        /// <param name="connection">The OLE DB connection.</param>
        public OleDbConnectionDataProvider(OleDbConnection connection)
            : this(connection, "[{0}]")
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="OleDbConnectionDataProvider"/> class
        /// using the specified OLE DB connection and identifier delimiter format.
        /// </summary>
        /// <param name="connection">The OLE DB connection.</param>
        /// <param name="identifierDelimiterFormat">The format for delimiting identifiers in queries.</param>
        public OleDbConnectionDataProvider(OleDbConnection connection, string identifierDelimiterFormat)
        {
            Connection = (OleDbConnection)(Provider.CloneConnection(connection));
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            IdentifierDelimiterFormat = identifierDelimiterFormat;
            SupportsAdvancedFiltering = false;
        }

        /// <inheritdoc />
        public sealed override bool SupportsAdvancedFiltering { get; set; }

        /// <summary>
        /// Gets or sets the supported <see cref="DbConnectionElementTypes"/> for the database connection.
        /// </summary>
        /// <remarks>
        /// This property indicates which element types (tables, views) are supported by this data provider.
        /// </remarks>
        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        /// <summary>
        /// Gets or sets the format used for delimiting identifiers in SQL queries.
        /// </summary>
        public string IdentifierDelimiterFormat { get; set; }

        /// <summary>
        /// Gets or sets the OLE DB connection associated with this provider.
        /// </summary>
        public new OleDbConnection Connection
        {
            get { return (OleDbConnection)base.Connection; }
            set { base.Connection = value; }
        }

        /// <summary>
        /// Gets or sets a value indicating whether the SQL dialect of Microsoft Access is used.
        /// </summary>
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public bool UseMsAccessSqlSyntax
        {
            get { return this.Provider.UseMsAccessSqlSyntax; }
            set { this.Provider.UseMsAccessSqlSyntax = value; }
        }

        /// <summary>
        /// Initializes the data provider by retrieving database schema information.
        /// </summary>
        protected override void Init()
        {
            if (Initialized)
                return;

            HashSet<string> excludedOwners = new HashSet<string>
            {
            "CTXSYS", "DBSNMP", "EXFSYS", "FLOWS_020100", "FLOWS_030000", "FLOWS_FILES", "IX",
            "LBACSYS", "MDSYS", "MGMT_VIEW", "OLAPSYS", "OWBSYS", "ORDPLUGINS", "ORDSYS",
            "OUTLN", "SI_INFORMTN_SCHEMA", "SYS", "SYSMAN", "SYSTEM", "TSMSYS", "WK_TEST",
            "WKSYS", "WKPROXY", "WMSYS", "XDB"
        };

            try
            {
                object[] restrictions;
                HashSet<string> passedTables = new HashSet<string>();

                if ((SupportedElementTypes & DbConnectionElementTypes.Table) != 0)
                {
                    DbCommandSetDataProviderHelper.SafeOpen(Connection);
                    restrictions = new Object[] { null, null, null, "TABLE" };
                    DbCommandSetDataProviderHelper.SafeOpen(Connection);
                    DataTable tableTables = Connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, restrictions);

                    bool containsSchemaInfo = tableTables.Columns.Contains("TABLE_SCHEMA");

                    foreach (DataRow dr in tableTables.Rows)
                    {
                        string tableName = dr["Table_Name"].ToString();
                        if (SuppressAddTableOrRelation(tableName, null))
                            continue;

                        if (containsSchemaInfo)
                        {
                            string tableSchema = dr["TABLE_SCHEMA"].ToString();
                            if (excludedOwners.Contains(tableSchema) || tableName.Contains("$"))
                                continue;
                        }

                        OleDbCommand command = new OleDbCommand(string.Format("SELECT * FROM " + IdentifierDelimiterFormat, tableName), Connection);
                        AddCommand(command, tableName, IdentifierDelimiterFormat, null);
                        passedTables.Add(tableName);
                    }
                }

                if ((SupportedElementTypes & DbConnectionElementTypes.View) != 0)
                {
                    DbCommandSetDataProviderHelper.SafeOpen(Connection);
                    restrictions = new Object[] { null, null, null, "VIEW" };
                    DbCommandSetDataProviderHelper.SafeOpen(Connection);
                    DataTable tableViews = Connection.GetOleDbSchemaTable(OleDbSchemaGuid.Tables, restrictions);

                    foreach (DataRow dr in tableViews.Rows)
                    {
                        string tableName = dr["Table_Name"].ToString();
                        if (SuppressAddTableOrRelation(tableName, null))
                            continue;

                        OleDbCommand command = new OleDbCommand(string.Format("SELECT * FROM " + IdentifierDelimiterFormat, tableName), Connection);
                        AddCommand(command, tableName, IdentifierDelimiterFormat, null);
                        passedTables.Add(tableName);
                    }
                }

                DbCommandSetDataProviderHelper.SafeOpen(Connection);
                restrictions = new Object[] { null, null, null, null };
                DataTable tableRelations = null;

                try
                {
                    tableRelations = Connection.GetOleDbSchemaTable(OleDbSchemaGuid.Foreign_Keys, restrictions);
                }
                catch (OleDbException)
                { }

                if (tableRelations != null)
                {
                    HashSet<string> passedRelationNames = new HashSet<string>();

                    for (int i = 0; i < tableRelations.Rows.Count; i++)
                    {
                        DataRow dr = tableRelations.Rows[i];
                        string childTableName = dr["FK_TABLE_NAME"].ToString();
                        string childColumnName = dr["FK_COLUMN_NAME"].ToString();
                        string parentTableName = dr["PK_TABLE_NAME"].ToString();
                        string parentColumnName = dr["PK_COLUMN_NAME"].ToString();

                        if (!passedTables.Contains(parentTableName) || !passedTables.Contains(childTableName))
                            continue;

                        if (SuppressAddTableOrRelation(parentTableName, null) || SuppressAddTableOrRelation(childTableName, null))
                            continue;

                        // look ahead for combined keys

                        int j = i + 1;
                        while (j < tableRelations.Rows.Count)
                        {
                            DataRow drNext = tableRelations.Rows[j];
                            if (drNext["PK_TABLE_NAME"].ToString() == parentTableName && drNext["FK_TABLE_NAME"].ToString() == childTableName)
                            {
                                // we have a hit - concatenate the key fields
                                childColumnName = String.Concat(childColumnName, "\t", drNext["FK_COLUMN_NAME"].ToString());
                                parentColumnName = String.Concat(parentColumnName, "\t", drNext["PK_COLUMN_NAME"].ToString());

                                // and skip this one
                                i++;
                                j++;
                            }
                            else
                            {
                                break;
                            }
                        }

                        string relationName = parentTableName + "2" + childTableName;
                        int relationIndex = 1;
                        string formatString = relationName + "{0}";

                        while (passedRelationNames.Contains(relationName))
                        {
                            relationName = string.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
                            relationIndex++;
                        }

                        passedRelationNames.Add(relationName);
                        AddRelation(relationName, parentTableName, childTableName, parentColumnName, childColumnName);
                    }
                }
            }
            finally
            {
                Connection.Close();
                Initialized = true;
            }
        }

        #region ISerializable Members

        /// <inheritdoc />
        protected OleDbConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("OleDbConnectionDataProvider.Version");
            if (version >= 1)
            {
                Connection = new OleDbConnection {
                    ConnectionString = info.GetString("ConnectionString")
                };
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
            }
            if (version >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
            if (version >= 3)
            {
                IdentifierDelimiterFormat = info.GetString("IdentifierDelimiterFormat");
            }
        }

        /// <inheritdoc />
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("OleDbConnectionDataProvider.Version", 3);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
            info.AddValue("IdentifierDelimiterFormat", IdentifierDelimiterFormat);
        }

        #endregion

        #region IFileList Members

        /// <inheritdoc />
        ReadOnlyCollection<string> IFileList.GetFileList()
        {
            string dataPattern = @"Data Source=.*?(?=;|$)";
            string data = Regex.Match(Connection.ConnectionString, dataPattern).Value.TrimEnd(';');
            string pathPattern = @"Data Source\s?=\s?";
            data = data.Replace(Regex.Match(data, pathPattern).Value, string.Empty);
            return new List<string> { data }.AsReadOnly();
        }

        /// <inheritdoc />
        void IFileList.SetFileList(ReadOnlyCollection<string> fileList)
        {
            string dataPattern = @"(?<prior>.+Data Source=)(?<file>.*?)(?<after>[;$].+)";
            Connection.ConnectionString = Regex.Replace(Connection.ConnectionString, dataPattern, $"${{prior}}{fileList[0]}${{after}}");
        }

        #endregion
    }
}