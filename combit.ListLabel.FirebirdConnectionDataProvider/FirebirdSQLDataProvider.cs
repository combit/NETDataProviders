using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Runtime.Serialization;
using System.Security.Permissions;
using FirebirdSql.Data.FirebirdClient;

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provider for FirebirdSQL, see http://www.firebirdsql.org/en/net-provider/
    /// </summary>
    /// <remarks>
    /// The <see cref="FirebirdSQLDataProvider"/> class extends <see cref="DbConnectionDataProvider"/> to provide connectivity
    /// to FirebirdSQL databases using the .NET provider. It retrieves schema information from the database and builds
    /// commands for retrieving data. It also supports custom filter syntax translation and native aggregate function mapping.
    /// </remarks>
    [Serializable]
    public sealed class FirebirdSQLDataProvider : DbConnectionDataProvider, ISerializable
    {
        // Private default constructor.
        private FirebirdSQLDataProvider()
            : base()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FirebirdSQLDataProvider"/> class from serialized data.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> containing the serialized object data.</param>
        /// <param name="context">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
        private FirebirdSQLDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("FirebirdSQLDataProvider.Version");
            if (version >= 1)
            {
                Connection = new FbConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                Provider.CancelBeforeClose = false;
            }
            if (version >= 2)
            {
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FirebirdSQLDataProvider"/> class using the specified database connection.
        /// </summary>
        /// <param name="connection">An <see cref="IDbConnection"/> instance representing a FirebirdSQL connection.</param>
        public FirebirdSQLDataProvider(IDbConnection connection)
        {
            Connection = connection;
            SupportedElementTypes = DbConnectionElementTypes.Table;
            Provider.CancelBeforeClose = false;
            SupportsAdvancedFiltering = false;
        }

        /// <summary>
        /// Gets or sets the supported element types (e.g. tables) for the database connection.
        /// </summary>
        public DbConnectionElementTypes SupportedElementTypes { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether advanced filtering is supported by the provider.
        /// </summary>
        public override bool SupportsAdvancedFiltering { get; set; }

        /// <summary>
        /// Initializes the data provider by retrieving table and relation schema information from the FirebirdSQL database.
        /// </summary>
        /// <remarks>
        /// This method retrieves a list of tables from the system catalog and then builds commands to select all data from each table.
        /// It then retrieves relation information from the schema and adds relations accordingly. Custom filter syntax translation 
        /// and native aggregate function mapping are also provided.
        /// </remarks>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        protected override void Init()
        {
            if (Initialized)
                return;

            HashSet<String> passedRelationNames = new HashSet<String>();
            HashSet<String> excludedRelations = new HashSet<String>();

            FbCommand cmd;

            try
            {
                // Get tables.
                string commandText = String.Format(CultureInfo.InvariantCulture,
                    "SELECT rdb$relation_name AS \"RelationName\" " +
                    "FROM rdb$relations " +
                    "WHERE rdb$system_flag is null or rdb$system_flag = 0");
                cmd = new FbCommand(commandText, Connection as FbConnection);

                if (Connection.State != ConnectionState.Open)
                    Connection.Open();
                FbDataReader reader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                while (reader.Read())
                {
                    if (!reader.IsDBNull(0))
                    {
                        string tableName = reader["RelationName"].ToString().Trim();
                        if (SuppressAddTableOrRelation(tableName, null))
                            continue;

                        FbConnection newConnection = (((Connection) as ICloneable).Clone() as FbConnection);
                        cmd = newConnection.CreateCommand();
                        cmd.CommandText = "SELECT * FROM " + tableName;
                        AddCommand(cmd, tableName, "\"{0}\"", "@{0}");
                    }
                }
                reader.Close();

                // Get relations.
                string relationCommandText = String.Format(CultureInfo.InvariantCulture,
                    "SELECT  rc2.rdb$relation_name AS \"PrimaryTable\", " +
                            "flds_pk.rdb$field_name AS \"PrimaryField\", " +
                            "rc.RDB$RELATION_NAME AS \"ForeignTable\", " +
                            "flds_fk.rdb$field_name AS \"ForeignField\", " +
                            "flds_pk.rdb$field_position AS \"Position\" " +
                    "FROM RDB$RELATION_CONSTRAINTS rc " +
                    "LEFT JOIN RDB$REF_CONSTRAINTS rfc ON (rc.RDB$CONSTRAINT_NAME = rfc.RDB$CONSTRAINT_NAME) " +
                    "LEFT JOIN RDB$INDEX_SEGMENTS flds_fk ON (flds_fk.RDB$INDEX_NAME = rc.RDB$INDEX_NAME) " +
                    "LEFT JOIN RDB$RELATION_CONSTRAINTS rc2 ON (rc2.RDB$CONSTRAINT_NAME = rfc.RDB$CONST_NAME_UQ) " +
                    "LEFT JOIN RDB$INDEX_SEGMENTS flds_pk ON ((flds_pk.RDB$INDEX_NAME = rc2.RDB$INDEX_NAME) AND (flds_fk.RDB$FIELD_POSITION = flds_pk.RDB$FIELD_POSITION)) " +
                    "WHERE rc.RDB$CONSTRAINT_TYPE = 'FOREIGN KEY' " +
                    "ORDER BY rc2.RDB$RELATION_NAME, flds_fk.RDB$FIELD_POSITION");

                cmd = new FbCommand(relationCommandText, Connection as FbConnection);

                if (Connection.State != ConnectionState.Open)
                    Connection.Open();
                FbDataReader relationReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);

                string lastRelationChildColumnName = "";
                string lastRelationParentColumnName = "";
                int counter = 0;

                while (relationReader.Read())
                {
                    if (!relationReader.IsDBNull(0) && !relationReader.IsDBNull(1))
                    {
                        string parentTableName = relationReader["PrimaryTable"].ToString().Trim();
                        string parentColumnName = relationReader["PrimaryField"].ToString().Trim();
                        string childTableName = relationReader["ForeignTable"].ToString().Trim();
                        string childColumnName = relationReader["ForeignField"].ToString().Trim();
                        if (parentTableName == childTableName)
                            continue;

                        if (SuppressAddTableOrRelation(parentTableName, null) || SuppressAddTableOrRelation(childTableName, null))
                            continue;

                        // Check for shared primary key.
                        if (Int16.Parse(relationReader["Position"].ToString()) > 0)
                        {
                            if (counter == 1)
                            {
                                lastRelationParentColumnName = parentColumnName;
                                lastRelationChildColumnName = childColumnName;
                            }
                            else
                            {
                                lastRelationParentColumnName += '\t' + parentColumnName;
                                lastRelationChildColumnName += '\t' + childColumnName;
                            }

                            if (counter == Int16.Parse(relationReader["Position"].ToString()))
                            {
                                parentColumnName = lastRelationParentColumnName;
                                childColumnName = lastRelationChildColumnName;
                                counter = 0;
                            }
                            else
                            {
                                continue;
                            }

                            ++counter;
                        }

                        string relName = parentTableName + "2" + childTableName;

                        if (excludedRelations.Contains(relName))
                            continue;

                        string reverseRelName = childTableName + "2" + parentTableName;
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
                relationReader.Close();
            }
            finally
            {
                Connection.Close();
                Initialized = true;
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
                case NativeAggregateFunction.StdDevPop:
                case NativeAggregateFunction.VarPop:
                case NativeAggregateFunction.VarSamp:
                    return null;
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
            info.AddValue("FirebirdSQLDataProvider.Version", 2);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }

        #endregion

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
                    switch (e.Name.ToString().ToUpper())
                    {
                        case "YEAR":
                            e.Result = String.Format("(EXTRACT(YEAR from {0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "MONTH":
                            e.Result = String.Format("(EXTRACT(MONTH from {0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "DAY":
                            e.Result = String.Format("(EXTRACT(DAY from {0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "MID$":
                            if (e.ArgumentCount == 2)
                                e.Result = (String.Format("(SUBSTRING({0} FROM {1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString() + "+1"));
                            else
                                e.Result = (String.Format("(SUBSTRING({0} FROM {1} FOR {2}))", e.Arguments[0].ToString(), e.Arguments[1].ToString() + "+1", e.Arguments[2].ToString()));
                            e.Handled = true;
                            break;
                        case "LEFT$":
                            e.Result = (String.Format("(LEFT({0}, {1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString()));
                            e.Handled = true;
                            break;
                        case "RIGHT$":
                            e.Result = (String.Format("(RIGHT({0}, {1}))", e.Arguments[0].ToString(), e.Arguments[1].ToString()));
                            e.Handled = true;
                            break;
                        case "LEN":
                            e.Result = (String.Format("(CHAR_LENGTH({0}))", e.Arguments[0].ToString()));
                            e.Handled = true;
                            break;
                        case "EMPTY":
                            if (e.ArgumentCount == 1)
                                e.Result = String.Format("(CHAR_LENGTH({0}) = 0)", e.Arguments[0]);
                            else
                                if ((bool)e.Arguments[1])
                                e.Result = String.Format("(CHAR_LENGTH(LTRIM(RTRIM({0}))) = 0)", e.Arguments[0]);
                            else
                                e.Result = String.Format("(CHAR_LENGTH({0}) = 0)", e.Arguments[0]);
                            e.Handled = true;
                            break;
                        case "ATRIM$":
                            e.Result = String.Format("(TRIM({0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "LTRIM$":
                            e.Result = String.Format("(TRIM(LEADING FROM {0}))", e.Arguments[0].ToString());
                            e.Handled = true;
                            break;
                        case "RTRIM$":
                            e.Result = String.Format("(TRIM(TRAILING FROM {0}))", e.Arguments[0].ToString());
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
    }
}
