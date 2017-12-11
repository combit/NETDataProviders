using NuoDb.Data.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace combit.ListLabel23.DataProviders
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
            if (info.GetInt32("NuoDBConnectionDataProvider.Version") >= 1)
            {
                Connection = new NuoDbConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
            }
            if (info.GetInt32("NuoDBConnectionDataProvider.Version") >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }

        }

        public NuoDbConnectionDataProvider(string connectionString)
            : this(new NuoDbConnection(connectionString)) { }


        public NuoDbConnectionDataProvider(NuoDbConnection connection)
        {
            Connection = connection;
            SupportedElementTypes = DbConnectionElementTypes.Table;
            Provider.CancelBeforeClose = false;
            SupportsAdvancedFiltering = true;
        }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public override bool SupportsAdvancedFiltering { get; set; }

        protected override void Init()
        {
            if (Initialized)
                return;

            List<String> passedRelationNames = new List<string>();
            List<String> passedTables = new List<string>();
            List<String> passedSchemas = new List<string>();
            List<String> excludedOwners = new List<string>();
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
                }
                if ((SupportedElementTypes & DbConnectionElementTypes.View) != 0)
                {
                    views = (Connection as NuoDbConnection).GetSchema("Views");
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

                        string name = dr["TABLE_NAME"].ToString();

                        ICloneable cloneable = (ICloneable)Connection;
                        Debug.Assert(cloneable != null);
                        if (cloneable != null)
                        {
                            NuoDbConnection newConnection = (NuoDbConnection)cloneable.Clone();
                            command = new NuoDbCommand("Select * From " + (String.IsNullOrEmpty(schema) ? name + "" : schema + "." + "\"" + name) + "\"", newConnection);
                            Provider.AddCommand(command, name, "{0}", "?");
                            passedTables.Add(name);
                        }
                        else
                            throw new LL_BadDatabaseStructure_Exception("The passed connection doesn't implement the ICloneable interface. Contact NuoDB support for an updated version.");
                    }
                }
                //get relations
                string commandText = String.Format(CultureInfo.InvariantCulture,
                                    "Select Distinct b.Tablename as PrimaryTable, c.Field as PrimaryField, " +
                                    " b2.Tablename as ForeignTable, c2.Field as ForeignField, a.Numberkeys as NumberKeys " +
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

                            if (!passedTables.Contains(childTableName))
                                continue;

                            string parentTableName = reader.GetString(0);

                            if (!passedTables.Contains(parentTableName))
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
                            Provider.AddRelation(relationName, parentTableName, childTableName, parentColumnName, childColumnName);
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
        #region ISerializable Members

        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
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
