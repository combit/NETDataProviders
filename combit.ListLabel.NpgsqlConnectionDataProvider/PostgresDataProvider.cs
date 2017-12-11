using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;
using Npgsql;

namespace combit.ListLabel23.DataProviders
{
    /// <summary>
    /// Provider for Npgsql Postgres connection, http://npgsql.projects.postgresql.org/
    /// Tested with version 2.0.4 for the .NET 2.0 framework
    /// </summary>
    [Serializable]
    public sealed class NpgsqlConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {
        public NpgsqlConnectionDataProvider(NpgsqlConnection connection)
        {
            Connection = connection;
            SupportedElementTypes = DbConnectionElementTypes.Table;
            Provider.CancelBeforeClose = false;
            SupportsAdvancedFiltering = true;
        }

        private NpgsqlConnectionDataProvider()
        {
        }

        private NpgsqlConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            if (info.GetInt32("NpgsqlConnectionDataProvider.Version") >= 1)
            {
                Connection = new NpgsqlConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
            }
            if (info.GetInt32("NpgsqlConnectionDataProvider.Version") >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }

        }

        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        public override bool SupportsAdvancedFiltering { get; set; }

        protected override void Init()
        {
            if (Initialized)
                return;

            NpgsqlCommand cmd;
            List<String> passedRelationNames = new List<string>();
            List<String> passedTableNames = new List<string>();

            Connection.Open();
            try
            {
                DataTable dt = (Connection as NpgsqlConnection).GetSchema("Tables");
                Connection.Close();
                foreach (DataRow dr in dt.Rows)
                {
                    string tableSchema = dr["TABLE_SCHEMA"].ToString();

                    if (tableSchema != "information_schema" && tableSchema != "pg_catalog")
                    {
                        string tableType = dr["TABLE_TYPE"].ToString();
                        string parentTableName = dr["TABLE_NAME"].ToString();

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
                        NpgsqlConnection newConnection = (Connection as NpgsqlConnection).Clone();

                        cmd = new NpgsqlCommand("Select * From \"" + (String.IsNullOrEmpty(tableSchema) ? parentTableName + "\"" : tableSchema + "\".\"" + parentTableName) + "\"", newConnection);
                        Provider.AddCommand(cmd, parentTableName, "\"{0}\"", ":{0}");
                        passedTableNames.Add(parentTableName);
                    }
                }
                string commandText = "SELECT a.table_name AS pk_table_name, a.column_name AS pk_colum_name, b.table_name AS fk_table_name, b.column_name AS fk_colum_name FROM information_schema.referential_constraints LEFT JOIN information_schema.key_column_usage AS a ON referential_constraints.constraint_name = a.constraint_name LEFT JOIN information_schema.key_column_usage AS b ON referential_constraints.unique_constraint_name= b.constraint_name";

                using (cmd = new NpgsqlCommand(commandText, Connection as NpgsqlConnection))
                {
                    Connection.Open();
                    NpgsqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        string childTableName = reader.GetString(0);

                        // do not add a relation if we haven't stored the associated child table (i.e. we want views)
                        if (!passedTableNames.Contains(childTableName))
                            continue;

                        string parentTableName = reader.GetString(2);

                        // do not add a relation if we haven't stored the associated parent table (i.e. we want views)
                        if (!passedTableNames.Contains(parentTableName))
                            continue;

                        string childColumnName = reader.GetString(1);
                        string parentColumnName = reader.GetString(3);

                        string relName = parentTableName + "2" + childTableName;
                        int relationIndex = 1;
                        string formatString = relName + "{0}";

                        while (passedRelationNames.Contains(relName))
                        {
                            relName = String.Format(CultureInfo.InvariantCulture, formatString, relationIndex);
                            relationIndex++;
                        }
                        passedRelationNames.Add(relName);
                        Provider.AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName);
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
                    {
                        switch (e.Name.ToString().ToUpper())
                        {
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
                            case "ROUND":
                                e.Result = String.Format("(ROUND({0}::numeric,{1}))", e.Arguments[0], e.ArgumentCount == 2 ? e.Arguments[1] : "0");
                                e.Handled = true;
                                break;
                            case "YEAR":
                                e.Result = String.Format("(date_part('year',{0}))", e.Arguments[0]);
                                e.Handled = true;
                                break;
                            case "MONTH":
                                e.Result = String.Format("(date_part('month',{0}))", e.Arguments[0]);
                                e.Handled = true;
                                break;
                            case "DAY":
                                e.Result = String.Format("(date_part('day',{0}))", e.Arguments[0]);
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

        #region ISerializable Members

        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("NpgsqlConnectionDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }

        #endregion
    }
}