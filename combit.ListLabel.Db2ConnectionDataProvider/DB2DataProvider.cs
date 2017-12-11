using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data.Common;
using System.Data;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Text.RegularExpressions;

namespace combit.ListLabel23.DataProviders
{
    /// <summary>
    /// Provider for DB2, see http://www-01.ibm.com/software/data/db2/ad/dotnet.html
    /// Tested with version 9.0.0.2
    /// </summary>
    [Serializable]
    public sealed class DB2ConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {
        private ReadOnlyCollection<String> _tableSchemas;

        private DB2ConnectionDataProvider()
        {
        }

        private DB2ConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            if (info.GetInt32("DB2ConnectionDataProvider.Version") >= 1)
            {
                Connection = Connection = CreateDbConnection(info.GetString("ConnectionString"));
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
                PrefixTableNameWithSchema = info.GetBoolean("PrefixTableNameWithSchema");
                _tableSchemas = (ReadOnlyCollection<string>)info.GetValue("TableSchemas", typeof(ReadOnlyCollection<string>));
            }
            if (info.GetInt32("DB2ConnectionDataProvider.Version") >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
        }

        public DB2ConnectionDataProvider(string connectionString)
            : this(connectionString, String.Empty)
        {
        }

        public DB2ConnectionDataProvider(string connectionString, ReadOnlyCollection<string> tableSchemas)
        {
            Connection = Connection = CreateDbConnection(connectionString);
            SupportedElementTypes = DbConnectionElementTypes.Table;
            _tableSchemas = tableSchemas;
            SupportsAdvancedFiltering = false;
            Provider.CancelBeforeClose = false;
        }

        public DB2ConnectionDataProvider(string connectionString, string tableSchema)
        {
            Connection = Connection = CreateDbConnection(connectionString);
            SupportedElementTypes = DbConnectionElementTypes.Table;
            List<string> schemas = new List<string>();
            if (!String.IsNullOrEmpty(tableSchema))
                schemas.Add(tableSchema);
            _tableSchemas = schemas.AsReadOnly();
            SupportsAdvancedFiltering = false;
            Provider.CancelBeforeClose = false;
        }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public bool PrefixTableNameWithSchema { get; set; }

        private static DbConnection CreateDbConnection(string connectionString)
        {
            // Assume failure.
            DbConnection connection = null;
            string providerName = "IBM.Data.DB2";

            // Create the DbProviderFactory and DbConnection.
            if (connectionString != null)
            {
                try
                {
                    DbProviderFactory factory = DbProviderFactories.GetFactory(providerName);

                    connection = factory.CreateConnection();
                    connection.ConnectionString = connectionString;
                }
                catch (Exception ex)
                {
                    // Set the connection to null if it was created.
                    if (connection != null)
                    {
                        connection = null;
                    }
                    Console.WriteLine(ex.Message);
                }
            }
            // Return the connection.
            return connection;
        }
        public override bool SupportsAdvancedFiltering { get; set; }
        protected override void Init()
        {
            if (Initialized)
                return;

            List<String> passedRelationNames = new List<string>();
            List<String> excludedOwners = new List<string>();
            excludedOwners.Add("SYSTOOLS");
            excludedOwners.Add("SYSIBM");
            excludedOwners.Add("SYSCAT");
            excludedOwners.Add("SYSSTAT");
            excludedOwners.Add("SYSIBMADM");
            
            //maybe someone need the sys-Views/Tables (admin user needed)
            foreach (string owner in _tableSchemas)
            {
                if (excludedOwners.Contains(owner))
                    excludedOwners.Remove(owner);
            }

            PrefixTableNameWithSchema = true;
            Connection.Open();
            DbCommand cmd;
            try
            {
                DataTable dt = (Connection as DbConnection).GetSchema("Tables");
                Connection.Close();
                foreach (DataRow dr in dt.Rows)
                {
                    string tableSchema = dr["TABLE_SCHEMA"].ToString();
                    string tableType = dr["TABLE_TYPE"].ToString();
                    string parentTableName = dr["TABLE_NAME"].ToString();

                    if (excludedOwners.Contains(tableSchema))
                        continue;

                    switch (tableType)
                    {
                        case "TABLE":
                            if ((SupportedElementTypes & DbConnectionElementTypes.Table) == 0)
                                continue;
                            break;

                        case "VIEW":
                            if ((SupportedElementTypes & DbConnectionElementTypes.View) == 0)
                                continue;
                            break;

                        default:
                            continue;
                    }
                    ICloneable cloneable = (ICloneable)Connection;
                    DbConnection newConnection = (DbConnection)cloneable.Clone();
                    cmd = newConnection.CreateCommand();
                    cmd.CommandText = "Select * From " + (String.IsNullOrEmpty(tableSchema) ? parentTableName + "" : tableSchema + "." + parentTableName) + "";
                    Provider.AddCommand(cmd, PrefixTableNameWithSchema ? String.Concat(tableSchema, ".", parentTableName) : parentTableName, "{0}", "?");
                }

                string commandText = "SELECT K.tabname, K.fk_colnames, K.reftabname, K.pk_colnames, K.tabschema, K.reftabschema FROM syscat.\"REFERENCES\" K";
                cmd = (Connection as DbConnection).CreateCommand();
                cmd.CommandText = commandText;
                using (cmd)
                {
                    Connection.Open();
                    DbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                    while (reader.Read())
                    {
                        if (!reader.IsDBNull(2) && !reader.IsDBNull(3))
                        {
                            string childColumnName = SplitString(reader.GetString(1));
                            string parentColumnName = SplitString(reader.GetString(3));
                            string childSchema = reader.GetString(4);
                            string parentSchema = reader.GetString(5);
                            string childTableName = SplitString(reader.GetString(0));
                            string parentTableName = SplitString(reader.GetString(2));
                            string relName = parentTableName + "2" + childTableName;
                            int relationIndex = 1;
                            string formatString = relName + "{0}";

                            while (passedRelationNames.Contains(relName))
                            {
                                relName = String.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
                                relationIndex++;
                            }
                            passedRelationNames.Add(relName);
                            Provider.AddRelation(relName, PrefixTableNameWithSchema ? String.Concat(parentSchema.Trim(), ".", parentTableName) : parentTableName, PrefixTableNameWithSchema ? String.Concat(childSchema.Trim(), ".", childTableName) : childTableName, parentColumnName, childColumnName);
                        }
                    }
                    reader.Close();
                }
            }
            finally
            {
                Connection.Close();
                Initialized = true;
            }
        }

        public string SplitString(string input)
        {
            string result = "";
            Regex r = new Regex("([^ ]+[ ]?[^ ]+)");
            MatchCollection mc = r.Matches(input);

            foreach (Match current in mc)
            {
                if (mc.Count > 1)
                    result += current.Groups[0] + "\t";
                else
                    result = current.Groups[0].ToString();
            }
            return result.TrimEnd('\t');
        }

        #region ISerializable Members

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
                        case "MID$":
                            if (e.ArgumentCount == 2)
                                e.Result = (String.Format("(SUBSTR({0},{1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString() + "+1"));
                            else
                                e.Result = (String.Format("(SUBSTR({0},{1},{2}))", e.Arguments[0].ToString(), e.Arguments[1].ToString() + "+1", e.Arguments[2].ToString()));
                            e.Handled = true;
                            break;
                        case "LEFT$":
                            e.Result = (String.Format("(LEFT({0},{1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString()));
                            e.Handled = true;
                            break;
                        case "RIGHT$":
                            e.Result = (String.Format("(RIGHT({0},{1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString()));
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

        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("DB2ConnectionDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("PrefixTableNameWithSchema", PrefixTableNameWithSchema);
            info.AddValue("TableSchemas", _tableSchemas);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }
        #endregion
    }
}