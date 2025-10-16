using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Linq;
using System.ComponentModel;

#if LLCP
using combit.Logging;
#endif

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provider for Npgsql Postgres connection, see http://npgsql.projects.postgresql.org/
    /// </summary>
    /// <remarks>
    /// The <see cref="NpgsqlConnectionDataProvider"/> class extends <see cref="DbConnectionDataProvider"/> to provide connectivity to PostgreSQL
    /// databases via the Npgsql .NET data provider. It supports tables and views, advanced filtering, and schema-aware query construction using
    /// customizable identifier delimiter and parameter marker formats. This provider is serializable and can be used as a data source for
    /// reporting engines such as List &amp; Label.
    /// </remarks>
    /// <example>
    /// The following example demonstrates how to use the <see cref="NpgsqlConnectionDataProvider"/> to export a report to PDF:
    /// <code language="csharp">
    /// // Initialize the NpgsqlConnectionDataProvider with your connection string.
    /// NpgsqlConnectionDataProvider provider = new NpgsqlConnectionDataProvider("your connection string here");
    /// 
    /// // Create an instance of the List &amp; Label reporting engine and assign the provider as its data source.
    /// using ListLabel listLabel = new ListLabel();
    /// listLabel.DataSource = provider;
    /// 
    /// // Configure export settings to generate a PDF.
    /// ExportConfiguration exportConfiguration = new ExportConfiguration(LlExportTarget.Pdf, @"C:\Exports\report.pdf", @"C:\Projects\report.llproj");
    /// exportConfiguration.ShowResult = true;
    /// 
    /// // Export the report to PDF.
    /// listLabel.Export(exportConfiguration);
    /// </code>
    /// </example>
    [Serializable]
    public sealed class NpgsqlConnectionDataProvider : DbConnectionDataProvider, ISerializable
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlConnectionDataProvider"/> class using the specified MySQL connection.
        /// </summary>
        /// <param name="connection">An open Npgsql connection.</param>
        public NpgsqlConnectionDataProvider(IDbConnection connection)
        {
            Connection = connection;
            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            Provider.CancelBeforeClose = false;
            SupportsAdvancedFiltering = true;
        }

        private NpgsqlConnectionDataProvider() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="NpgsqlConnectionDataProvider"/> class from serialized data.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> containing the serialized data.</param>
        /// <param name="context">The <see cref="StreamingContext"/> for this serialization.</param>
        private NpgsqlConnectionDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("NpgsqlConnectionDataProvider.Version");
            if (version >= 1)
            {
                Connection = new NpgsqlConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
            }
            if (version >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
                PrefixTableNameWithSchema = info.GetBoolean("PrefixTableNameWithOwner");
            }
        }

        /// <summary>
        /// Gets or sets the supported <see cref="DbConnectionElementTypes"/> for the database connection.
        /// </summary>
        /// <remarks>
        /// This property indicates which element types (tables, views) are supported by this data provider.
        /// </remarks>
        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether table names should be prefixed with the schema.
        /// </summary>
        public bool PrefixTableNameWithSchema { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether advanced filtering is supported by this provider.
        /// </summary>
        public override bool SupportsAdvancedFiltering { get; set; }

        /// <summary>
        /// Initializes the data provider by retrieving schema information from the PostgreSQL database.
        /// </summary>
        /// <remarks>
        /// The method retrieves tables and views using the GetSchema method of the connection, merges them if necessary,
        /// and builds commands to retrieve all data from each table. It also retrieves relation information from the
        /// information_schema and adds relations accordingly.
        /// </remarks>
        protected override void Init()
        {
            if (Initialized)
                return;

            HashSet<string> passedRelationNames = new HashSet<string>();

            Connection.Open();

            try
            {
                bool containsViews = false;
                DataTable dtTables;

                if ((SupportedElementTypes & DbConnectionElementTypes.Table) != 0)
                {
                    dtTables = (Connection as NpgsqlConnection).GetSchema("Tables");
                }
                else
                {
                    dtTables = new DataTable();
                }

                // Getschema("Tables") may supply only tables; if views are supported, merge them.
                if ((SupportedElementTypes & DbConnectionElementTypes.View) != 0)
                {
                    try
                    {
                        DataTable dtViews = (Connection as NpgsqlConnection).GetSchema("Views");
                        // Merge the DataTables.
                        if (dtViews.Rows.Count > 0)
                        {
                            dtViews.Columns["check_option"].ColumnName = "table_type";
                            dtViews.Columns.Remove("is_updatable");
                            // Update the table_type column.
                            var changeColumnName = dtViews.AsEnumerable()
                                .Select(row =>
                                {
                                    row["table_type"] = "VIEW";
                                    return row;
                                });
                            dtViews = changeColumnName.CopyToDataTable();
                            // Remove duplicate views if present.
                            if (dtTables.AsEnumerable().Any(row => "VIEW" == row.Field<string>("table_type")))
                                containsViews = true;

                            // Merge the views into the tables DataTable.
                            dtTables.Merge(dtViews);

                            // Remove duplicates.
                            if (containsViews)
                            {
                                var uniqueTables = dtTables.AsEnumerable()
                                    .GroupBy(row => row.Field<string>("TABLE_NAME"))
                                    .Select(row => row.First());
                                dtTables = uniqueTables.CopyToDataTable();
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        LoggingHelper.LogExceptionDetails(ex, Logger);
                    }
                }

                DataProviderHelper.LogDataTableStructure(Logger, dtTables);

                Connection.Close();
                Provider.PrefixTableNameWithSchema = PrefixTableNameWithSchema;

                foreach (DataRow dr in dtTables.Rows)
                {
                    string tableSchema = dr["TABLE_SCHEMA"].ToString();

                    if (tableSchema != "information_schema" && tableSchema != "pg_catalog")
                    {
                        string tableType = dr["TABLE_TYPE"].ToString();
                        string parentTableName = dr["TABLE_NAME"].ToString();

                        if (SuppressAddTableOrRelation(parentTableName, tableSchema))
                            continue;

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

                        NpgsqlConnection newConnection;
                        if (Connection is ICloneable)   // For Npgsql versions less than 3.0.0.
                        {
                            newConnection = (NpgsqlConnection)((Connection as ICloneable).Clone());
                        }
                        else  // For Npgsql versions 3.0.0 and above.
                        {
                            newConnection = new NpgsqlConnection(Connection.ConnectionString);
                        }

                        string txt;
                        if (string.IsNullOrEmpty(tableSchema))
                        {
                            txt = String.Format("Select * From \"{0}\"", parentTableName);
                        }
                        else
                        {
                            txt = String.Format("Select * From \"{0}\".\"{1}\"", tableSchema, parentTableName);
                        }

                        AddCommand(new NpgsqlCommand(txt, newConnection), parentTableName, "\"{0}\"", ":{0}");
                    }
                }

                string commandText = "SELECT a.table_name AS pk_table_name, a.column_name AS pk_colum_name, b.table_name AS fk_table_name, b.column_name AS fk_colum_name, a.table_schema, b.table_schema FROM information_schema.referential_constraints LEFT JOIN information_schema.key_column_usage AS a ON referential_constraints.constraint_name = a.constraint_name LEFT JOIN information_schema.key_column_usage AS b ON referential_constraints.unique_constraint_name= b.constraint_name";

                using (NpgsqlCommand cmd = new NpgsqlCommand(commandText, Connection as NpgsqlConnection))
                {
                    Connection.Open();
                    NpgsqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        string childTableName = reader.GetString(0);
                        string childColumnName = reader.GetString(1);
                        string parentTableName = reader.GetString(2);
                        string parentColumnName = reader.GetString(3);
                        string parentSchema = reader.GetString(4);
                        string childSchema = reader.GetString(5);

                        // Check existence of both tables.
                        if (!dtTables.AsEnumerable().Any(row => childTableName == row.Field<string>("TABLE_NAME")) ||
                            !dtTables.AsEnumerable().Any(row => parentTableName == row.Field<string>("TABLE_NAME")))
                            continue;

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
                        AddRelation(relName, parentTableName, childTableName, parentColumnName, childColumnName, parentSchema, childSchema);
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

        /// <summary>
        /// Translates filter syntax from List &amp; Label expressions to FirebirdSQL-specific syntax.
        /// </summary>
        /// <param name="sender">The source of the filter translation request.</param>
        /// <param name="e">A <see cref="TranslateFilterSyntaxEventArgs"/> object that contains the event data.</param>
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

        /// <summary>
        /// Gets the native aggregate function name for the specified aggregate function.
        /// </summary>
        /// <param name="function">The aggregate function to be mapped.</param>
        /// <returns>
        /// The native aggregate function name as used by FirebirdSQL, or <c>null</c> for unsupported functions.
        /// </returns>
        protected override string GetNativeAggregateFunctionName(NativeAggregateFunction function)
        {
            switch (function)
            {
                case NativeAggregateFunction.StdDevSamp:
                    return "STDDEV_SAMP";
                case NativeAggregateFunction.StdDevPop:
                    return "STDDEV_POP";
                case NativeAggregateFunction.VarSamp:
                    return "VAR_SAMP";
                case NativeAggregateFunction.VarPop:
                    return "VAR_POP";
                default:
                    return function.ToString().ToUpperInvariant();
            }
        }

        #region ISerializable Members

        /// <inheritdoc/>
#if !NET_BUILD
    [SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
#endif
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("NpgsqlConnectionDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
            info.AddValue("PrefixTableNameWithOwner", PrefixTableNameWithSchema);
        }
        #endregion
    }
}