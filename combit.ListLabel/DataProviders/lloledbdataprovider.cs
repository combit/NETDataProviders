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
    [Serializable]
    public class OleDbConnectionDataProvider : DbConnectionDataProvider, ISerializable, IFileList
    {
        public OleDbConnectionDataProvider()
            : base()
        { }

        public OleDbConnectionDataProvider(OleDbConnection connection)
            : this(connection, "[{0}]")
        { }

        public OleDbConnectionDataProvider(OleDbConnection connection, string identifierDelimiterFormat)
        {
            Connection = (OleDbConnection)(Provider.CloneConnection(connection));
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            IdentifierDelimiterFormat = identifierDelimiterFormat;
            SupportsAdvancedFiltering = false;
        }

        public sealed override bool SupportsAdvancedFiltering { get; set; }
        public DbConnectionElementTypes SupportedElementTypes { get; set; }
        public string IdentifierDelimiterFormat { get; set; }
        public new OleDbConnection Connection
        {
            get { return (OleDbConnection)base.Connection; }
            set { base.Connection = value; }
        }


        /// <summary>If enabled, the SQL dialect of Microsoft Access is used.</summary> 
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public bool UseMsAccessSqlSyntax
        {
            get { return this.Provider.UseMsAccessSqlSyntax; }
            set { this.Provider.UseMsAccessSqlSyntax = value; }
        }

        protected override void Init()
        {
            if (Initialized)
                return;

            HashSet<String> excludedOwners = new HashSet<string>();

            // see http://download.oracle.com/docs/cd/B28359_01/server.111/b28337/tdpsg_user_accounts.htm#BABGEGFI

            excludedOwners.Add("CTXSYS");
            excludedOwners.Add("DBSNMP");
            excludedOwners.Add("EXFSYS");
            excludedOwners.Add("FLOWS_020100");
            excludedOwners.Add("FLOWS_030000");
            excludedOwners.Add("FLOWS_FILES");
            excludedOwners.Add("IX");
            excludedOwners.Add("LBACSYS");
            excludedOwners.Add("MDSYS");
            excludedOwners.Add("MGMT_VIEW");
            excludedOwners.Add("OLAPSYS");
            excludedOwners.Add("OWBSYS");
            excludedOwners.Add("ORDPLUGINS");
            excludedOwners.Add("ORDSYS");
            excludedOwners.Add("OUTLN");
            excludedOwners.Add("SI_INFORMTN_SCHEMA");
            excludedOwners.Add("SYS");
            excludedOwners.Add("SYSMAN");
            excludedOwners.Add("SYSTEM");
            excludedOwners.Add("TSMSYS");
            excludedOwners.Add("WK_TEST");
            excludedOwners.Add("WKSYS");
            excludedOwners.Add("WKPROXY");
            excludedOwners.Add("WMSYS");
            excludedOwners.Add("XDB");

            try
            {
                // parse all tables
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

                        // Oracle
                        if (containsSchemaInfo)
                        {
                            string tableSchema = dr["TABLE_SCHEMA"].ToString();

                            // Logger.Debug("Table = {3}, Schema='{0}', Contained={1}", LlLogCategory.DataProvider, tableSchema, excludedOwners.Contains(tableSchema), tableName);

                            if (excludedOwners.Contains(tableSchema))
                                continue;

                            if (tableName.Contains("$"))
                                continue;
                        }

                        OleDbCommand command = new OleDbCommand(String.Format("SELECT * FROM " + IdentifierDelimiterFormat, tableName), Connection);
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

                    //    foreach (DataRow dr in tableViews.Rows)
                    //    {
                    //        Logger.Debug("Dump info for view {0}", dr["Table_Name"].ToString());
                    //        foreach (DataColumn col in tableViews.Columns)
                    //        {
                    //            Logger.Debug("{0}/{1}", col.ColumnName, dr[col].ToString());
                    //        }
                    //        Logger.Debug("---------------------------");
                    //    }

                    foreach (DataRow dr in tableViews.Rows)
                    {
                        string tableName = dr["Table_Name"].ToString();
                        if (SuppressAddTableOrRelation(tableName, null))
                            continue;

                        OleDbCommand command = new OleDbCommand(String.Format("SELECT * FROM " + IdentifierDelimiterFormat, tableName), Connection);
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
                catch (OleDbException) // relations not supported
                { }

                HashSet<String> passedRelationNames = new HashSet<string>();

                // parse all relations
                if (tableRelations != null)
                {
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
                            relationName = String.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
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
        protected OleDbConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("OleDbConnectionDataProvider.Version");
            if (version >= 1)
            {
                Connection = new OleDbConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
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

#if !NET_BUILD
        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
#endif
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
        public System.Collections.ObjectModel.ReadOnlyCollection<string> GetFileList()
        {
            string dataPattern = @"Data Source=.*?(?=;|$)";
            string data = Regex.Match(Connection.ConnectionString, dataPattern).Value.TrimEnd(';');

            string pathPattern = @"Data Source\s?=\s?";
            string prefix = Regex.Match(data, pathPattern).Value;
            data = data.Replace(prefix, String.Empty);

            List<string> files = new List<string>();
            files.Add(data);
            return files.AsReadOnly();
        }

        public void SetFileList(ReadOnlyCollection<string> fileList)
        {
            string dataPattern = @"(?<prior>.+Data Source=)(?<file>.*?)(?<after>[;$].+)";
            Connection.ConnectionString = Regex.Replace(Connection.ConnectionString, dataPattern, String.Concat("${prior}", fileList[0], "${after}"));
        }
#endregion
    }
}