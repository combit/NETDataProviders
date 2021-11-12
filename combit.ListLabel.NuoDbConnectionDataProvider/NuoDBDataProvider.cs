using NuoDb.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provider for NuoDB, see http://www.nuodb.com/devcenter/
    /// Tested with NuoDb.Data.Client version 1.1.0.4
    /// </summary>
    [Serializable]
    public sealed class NuoDbConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {
        private NuoDbConnectionDataProvider()
        {
        }

        private NuoDbConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("NuoDBConnectionDataProvider.Version");
            if (version >= 1)
            {
                Connection = new NuoDbConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
            }
            if (version >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }

        }

        public NuoDbConnectionDataProvider(string connectionString)
            : this(new NuoDbConnection(connectionString)) { }

#pragma warning disable CS3001
        public NuoDbConnectionDataProvider(NuoDbConnection connection)
#pragma warning restore CS3001
        {
            Connection = connection;
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            Provider.CancelBeforeClose = false;
            SupportsAdvancedFiltering = true;
        }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public override bool SupportsAdvancedFiltering { get; set; }

        protected override void Init()
        {
            if (Initialized)
                return;

            HashSet<String> passedRelationNames = new HashSet<string>();
            HashSet<String> passedSchemas = new HashSet<string>();
            HashSet<String> excludedOwners = new HashSet<string>();
            excludedOwners.Add("System".ToUpper());

            if (Connection.State != ConnectionState.Open)
                Connection.Open();
            NuoDbCommand command;
            try
            {
                DataTable views = null;
                DataTable tables = null;
                if ((SupportedElementTypes & DbConnectionElementTypes.Table) != 0)
                {
                    tables = (Connection as NuoDbConnection).GetSchema("Tables");
                    DataProviderHelper.LogDataTableStructure(Logger, tables);
                }
                if ((SupportedElementTypes & DbConnectionElementTypes.View) != 0)
                {
                    views = (Connection as NuoDbConnection).GetSchema("Views");
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
                            Debug.Assert(false);
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

                        // Get schema from connection string
                        NuoDbConnection connection = (NuoDbConnection)Connection;
                        NuoDbConnectionStringBuilder _parsedConnectionString = new NuoDbConnectionStringBuilder(Connection.ConnectionString);
                        string usedSchema;
                        if (Connection.ConnectionString.Contains("schema"))
                        {
                            usedSchema = _parsedConnectionString.Schema;
                        }
                        else
                        {
                            usedSchema = String.Empty;
                        }
                        // If no schema is specified in the connectionString, get all tables of all schemas,
                        // otherwise just get the tables of the specified schema
                        if ((schema != usedSchema.ToUpper()) && !String.IsNullOrEmpty(usedSchema))
                            continue;

                        // No schema specified, add them to list, to build relations
                        if (!passedSchemas.Contains(schema))
                        {
                            passedSchemas.Add(schema);
                        }

                        ICloneable cloneable = (ICloneable)Connection;
                        Debug.Assert(cloneable != null);
                        if (cloneable != null)
                        {
                            NuoDbConnection newConnection = (NuoDbConnection)cloneable.Clone();
                            command = new NuoDbCommand("Select * From " + (String.IsNullOrEmpty(schema) ? name + "" : schema + "." + "\"" + name) + "\"", newConnection);
                            AddCommand(command, name, "\"{0}\"", "?");
                        }
                        else
                            throw new LL_BadDatabaseStructure_Exception("The passed connection doesn't implement the ICloneable interface. Contact NuoDB support for an updated version.");
                    }
                }
                //get relations
                string commandText = String.Format(CultureInfo.InvariantCulture,
                                    "Select Distinct b.Tablename as PrimaryTable, c.Field as PrimaryField, " +
                                    " b2.Tablename as ForeignTable, c2.Field as ForeignField, a.Numberkeys as NumberKeys, b.schema, b2.schema " +
                                    "From System.Foreignkeys a " +
                                    "Left outer join System.Tables b " +
                                    "on a.PrimaryTableId =b.Tableid " +
                                    "Left outer join System.Fields c " +
                                    "on a.PrimaryFieldId = c.FieldId " +
                                    "Left outer join System.Tables b2 " +
                                    "on a.ForeignTableid =b2.TableId " +
                                    "Left outer join System.Fields c2 " +
                                    "on a.ForeignFieldId =c2.FieldId " +
                                    "where b.Tablename =c.Tablename " +
                                    "and " +
                                    "b2.Tablename = c2.Tablename ");

                using (command = new NuoDbCommand(commandText, Connection as NuoDbConnection))
                {
                    string lastRelationChildColumnName = "";
                    string lastRelationParentColumnName = "";
                    int counter = 0;
                    Connection.Open();
                    DbDataReader reader = command.ExecuteReader(CommandBehavior.CloseConnection);

                    while (reader.Read())
                    {
                        if (!reader.IsDBNull(0) && !reader.IsDBNull(1))
                        {
                            string childColumnName = reader.GetString(3);
                            string parentColumnName = reader.GetString(1);
                            string childTableName = reader.GetString(2);
                            string parentTableName = reader.GetString(0);
                            string parentSchema = reader.GetString(5);
                            string childSchema = reader.GetString(6);

                            if (excludedOwners.Contains(parentSchema) || excludedOwners.Contains(childSchema))
                                continue;

                            if (SuppressAddTableOrRelation(parentTableName, parentSchema) || SuppressAddTableOrRelation(childTableName, childSchema))
                                continue;

                            //check whether shared primary key
                            if (reader.GetInt16(4) > 1)
                            {
                                ++counter;
                                //first time i am empty
                                if (counter == 1)
                                {
                                    lastRelationParentColumnName = parentColumnName;
                                    lastRelationChildColumnName = childColumnName;
                                }
                                else
                                {
                                    lastRelationChildColumnName += '\t' + childColumnName;
                                    lastRelationParentColumnName += '\t' + parentColumnName;
                                }

                                if (counter == reader.GetInt16(4))
                                {
                                    parentColumnName = lastRelationParentColumnName;
                                    childColumnName = lastRelationChildColumnName;
                                    counter = 0;
                                }
                                else
                                {
                                    continue;
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
                    reader.Close();
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
                        case "LEN":
                            if (e.ArgumentCount == 1)
                            e.Result = (String.Format("Length({0})", e.Arguments[0].ToString()));  
                            e.Handled = true;
                            break;
                        case "ATRIM$":
                            e.Result = (String.Format("Trim({0})", e.Arguments[0].ToString()));  
                            e.Handled = true;
                            break;
                        case "LTRIM$":
                            e.Result = (String.Format("LTrim({0})", e.Arguments[0].ToString()));  
                            e.Handled = true;
                            break;
                        case "RTRIM$":
                            e.Result = (String.Format("RTrim({0})", e.Arguments[0].ToString()));
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

        //http://doc.nuodb.com/display/doc/Aggregate+Functions
        protected override string GetNativeAggregateFunctionName(NativeAggregateFunction function)
        {
            switch (function)
            {
                case NativeAggregateFunction.StdDevSamp:
                case NativeAggregateFunction.StdDevPop:
                case NativeAggregateFunction.VarPop:
                case NativeAggregateFunction.VarSamp:
                    return null;

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
            info.AddValue("NuoDBConnectionDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }

        #endregion
    }
}
