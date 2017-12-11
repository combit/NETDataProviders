using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace combit.ListLabel23.DataProviders
{
    /// <summary>
    /// Provider for MySql, see http://www.mysql.de/products/connector/
    /// Tested with version 6.0.3
    /// </summary>
    [Serializable]
    public sealed class MySqlConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {
        private MySqlConnectionDataProvider()
        {
        }

        private MySqlConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            if (info.GetInt32("MySqlConnectionDataProvider.Version") >= 1)
            {
                Connection = CreateDbConnection(info.GetString("ConnectionString"));
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
            }
            if (info.GetInt32("MySqlConnectionDataProvider.Version") >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
        }

        public MySqlConnectionDataProvider(string connectionString)
        {
            Connection = CreateDbConnection(connectionString);
            SupportedElementTypes = DbConnectionElementTypes.Table;
            Provider.CancelBeforeClose = false;
            SupportsAdvancedFiltering = true;
        }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public override bool SupportsAdvancedFiltering { get; set; }

        private static DbConnection CreateDbConnection(string connectionString)
        {
            // Assume failure.
            DbConnection connection = null;
            string providerName = "MySql.Data.MySqlClient";

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

        protected override void Init()
        {
            if (Initialized)
                return;

            List<String> passedRelationNames = new List<string>();
            List<String> passedSchemas = new List<string>();
            List<String> excludedOwners = new List<string>();
            excludedOwners.Add("mysql");
            excludedOwners.Add("information_schema");

            if (Connection.State != ConnectionState.Open)
                Connection.Open();

            DbCommand cmd;
            try
            {
                DataTable views = null;
                DataTable tables = null;
                if ((SupportedElementTypes & DbConnectionElementTypes.Table) != 0)
                {
                    tables = (Connection as DbConnection).GetSchema("Tables");
                }
                if ((SupportedElementTypes & DbConnectionElementTypes.View) != 0)
                {
                    views = (Connection as DbConnection).GetSchema("Views");
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
                    }
                }

                foreach (string schema in passedSchemas)
                {
                    string commandText = String.Format(CultureInfo.InvariantCulture, "SELECT K.`TABLE_NAME`, K.`COLUMN_NAME`, K.`REFERENCED_TABLE_NAME`, K.`REFERENCED_COLUMN_NAME` FROM information_schema.KEY_COLUMN_USAGE K WHERE `TABLE_SCHEMA`='{0}'", schema);
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
                                string childColumnName = reader.GetString(1);
                                string parentColumnName = reader.GetString(3);
                                string childTableName = reader.GetString(0);
                                string parentTableName = reader.GetString(2);
                                if (SuppressAddTableOrRelation(parentTableName, null) || SuppressAddTableOrRelation(childTableName, null))
                                    continue;

                                string relName = parentTableName + "2" + childTableName;
                                int relationIndex = 1;
                                string formatString = relName + "{0}";

                                while (passedRelationNames.Contains(relName))
                                {
                                    relName = String.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
                                    relationIndex++;
                                }
                                passedRelationNames.Add(relName);
                                AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName);
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

        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("MySqlConnectionDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }

        #endregion
    }
}