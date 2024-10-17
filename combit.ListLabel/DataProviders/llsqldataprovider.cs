using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
#if NET_BUILD
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace combit.Reporting.DataProviders
{
    [Serializable]
    [DataProviderThreadSafenessAttribute(DataProviderThreadSafeness = DataProviderThreadSafeness.Full)]
    public class SqlConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {
        protected ReadOnlyCollection<String> TableSchemas { get; set; }

        public SqlConnectionDataProvider(SqlConnection connection)
            : this(connection, String.Empty) { }

        public SqlConnectionDataProvider(SqlConnection connection, string tableSchema)
        {
            Connection = (Provider.CloneConnection(connection));
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            List<string> schemas = new List<string>();
            if (!String.IsNullOrEmpty(tableSchema))
                schemas.Add(tableSchema);
            TableSchemas = schemas.AsReadOnly();

            // set default-value for property
            PrefixTableNameWithSchema = false;
            SupportsAdvancedFiltering = true;
            InitSqlModifications();
        }

        public SqlConnectionDataProvider(SqlConnection connection, ReadOnlyCollection<string> tableSchemas)
        {
            Connection = (Provider.CloneConnection(connection));
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            TableSchemas = tableSchemas;

            // set default-value for property
            PrefixTableNameWithSchema = false;
            SupportsAdvancedFiltering = true;
            InitSqlModifications();
        }


        protected SqlConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("SqlConnectionDataProvider.Version");
            if (version >= 1)
            {
                Connection = new SqlConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                PrefixTableNameWithSchema = info.GetBoolean("PrefixTableNameWithSchema");
                TableSchemas = (ReadOnlyCollection<string>)info.GetValue("TableSchemas", typeof(ReadOnlyCollection<string>));
            }
            if (version >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
            InitSqlModifications();
        }

        protected SqlConnectionDataProvider()
            : base() { }

        #region ISerializable Members
#if !NET_BUILD
        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
#endif
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("SqlConnectionDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("PrefixTableNameWithSchema", PrefixTableNameWithSchema);
            info.AddValue("TableSchemas", TableSchemas);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }

        #endregion

        protected void InitSqlModifications()
        {
            Provider.RegExForTableName = @"^[\p{L}\p{N} \.\-_\$%&#/\t\[\]\(\)]+$"; // allows unicode letters + numbers, [space], ., -, $, %, /, _, &, #, [, ], (, )
            Func<string, string> identifierModificator = identifier =>
             {
                 return identifier.Replace("\"", "\"\"");
             };
            Provider.IdentifierModificator = identifierModificator;
        }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public bool PrefixTableNameWithSchema { get; set; }

        public override bool SupportsAdvancedFiltering { get; set; }

        protected override void Init()
        {
            if (Initialized)
                return;

            HashSet<String> passedRelationNames = new HashSet<string>();
            Provider.PrefixTableNameWithSchema = PrefixTableNameWithSchema;

            Connection.Open();
            try
            {
                DataTable dt = (Connection as SqlConnection).GetSchema("Tables");
                DataProviderHelper.LogDataTableStructure(Logger, dt);

                // now get the constraints
                string sqlConstraints =
                @"select
                    PKTABLE_OWNER = convert(sysname, schema_name(o1.schema_id)),
                    PKTABLE_NAME = convert(sysname, o1.name),
                    PKCOLUMN_NAME = convert(sysname, c1.name),
                    FKTABLE_OWNER = convert(sysname, schema_name(o2.schema_id)),
                    FKTABLE_NAME = convert(sysname, o2.name),
                    FKCOLUMN_NAME = convert(sysname, c2.name),
                    --Force the column to be non - nullable(see SQL BU 325751)
                    KEY_SEQ = isnull(convert(smallint, k.constraint_column_id), 0),
                    f.name as RelationName
                from
                    sys.objects o1,
                    sys.objects o2,
                    sys.columns c1,
                    sys.columns c2,
                    sys.foreign_keys f inner join
                    sys.foreign_key_columns k on(k.constraint_object_id = f.object_id) inner join
                    sys.indexes i on(f.referenced_object_id = i.object_id and f.key_index_id = i.index_id)
                where
                    o1.object_id = f.referenced_object_id and
                    o2.object_id = f.parent_object_id and
                    c1.object_id = f.referenced_object_id and
                    c2.object_id = f.parent_object_id and
                    c1.column_id = k.referenced_column_id and
                    c2.column_id = k.parent_column_id
                order by
                    PKTABLE_OWNER, PKTABLE_NAME, FKTABLE_OWNER, FKTABLE_NAME, RelationName, KEY_SEQ";

                SqlCommand constraintsCommand = new SqlCommand(sqlConstraints, Connection as SqlConnection);
                SqlDataAdapter adapter = new SqlDataAdapter(constraintsCommand);
                DataTable constraints = new DataTable();
                adapter.Fill(constraints);

                foreach (DataRow dr in dt.Rows)
                {
                    string tableSchema = dr["TABLE_SCHEMA"].ToString();
                    string parentTableName = dr["TABLE_NAME"].ToString();

                    if (SuppressAddTableOrRelation(parentTableName, tableSchema))
                        continue;

                    if ((TableSchemas.Count != 0) && !(TableSchemas.Contains(tableSchema)))
                        continue;

                    string tableType = dr["TABLE_TYPE"].ToString();

                    switch (tableType)
                    {
                        case "BASE TABLE":
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

                    // pass table
                    SqlConnection newConnection = Provider.CloneConnection(Connection) as SqlConnection;
                    SqlCommand cmd = new SqlCommand("SELECT * FROM " + DbCommandSetDataProviderHelper.GetTableDotColumn("\"{0}\"", tableSchema, parentTableName, null, null, false, Provider.IdentifierModificator), newConnection);
                    AddCommand(cmd, parentTableName, "\"{0}\"", null);

                    // pass relations
                    string rowFilter = String.Format("PKTABLE_NAME='{0}'", parentTableName);

                    if (!String.IsNullOrEmpty(tableSchema))
                    {
                        rowFilter += String.Format(" and PKTABLE_OWNER='{0}'", tableSchema);
                    }

                    DataRow[] matchingRows = constraints.Select(rowFilter);

                    foreach (DataRow row in matchingRows)
                    {
                        string childSchema = row["FKTABLE_OWNER"].ToString();
                        string childTableName = row["FKTABLE_NAME"].ToString();

                        if (SuppressAddTableOrRelation(parentTableName, tableSchema) || SuppressAddTableOrRelation(childTableName, childSchema))
                            continue;

                        string childColumnName = row["FKCOLUMN_NAME"].ToString();
                        string parentColumnName = row["PKCOLUMN_NAME"].ToString();

                        if ((TableSchemas.Count != 0) && !(TableSchemas.Contains(childSchema)))
                            continue;

                        // combined primary key, add key field to last relation on stack
                        if (Convert.ToInt32(row["KEY_SEQ"]) > 1)
                        {
                            IDataProvider providerInterface = (Provider as IDataProvider);
                            DbCommandTableRelation lastRelation = providerInterface.Relations[providerInterface.Relations.Count - 1] as DbCommandTableRelation;
                            lastRelation.ChildColumnName += '\t' + childColumnName;
                            lastRelation.ParentColumnName += '\t' + parentColumnName;
                            continue;
                        }

                        string relName = row["PKTABLE_NAME"].ToString() + "2" + childTableName;
                        int relationIndex = 1;
                        string formatString = relName + "{0}";

                        while (passedRelationNames.Contains(relName))
                        {
                            relName = String.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
                            relationIndex++;
                        }
                        passedRelationNames.Add(relName);

                        AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName, tableSchema, childSchema);
                    }
                }
            }
            finally
            {
                Connection.Close();
                Initialized = true;
            }
        }

        //https://msdn.microsoft.com/de-de/library/ms173454.aspx
        protected override string GetNativeAggregateFunctionName(NativeAggregateFunction functionInstance)
        {
            switch (functionInstance)
            {
                case NativeAggregateFunction.Count:
                    return "COUNT_BIG";
                case NativeAggregateFunction.StdDevSamp:
                    return "STDEV";
                case NativeAggregateFunction.StdDevPop:
                    return "STDEVP";
                case NativeAggregateFunction.VarSamp:
                    return "VAR";
                case NativeAggregateFunction.VarPop:
                    return "VARP";
                default:
                    return functionInstance.ToString().ToUpperInvariant();
            }
        }
    }
}