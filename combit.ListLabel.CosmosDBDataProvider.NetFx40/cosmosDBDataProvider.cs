using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

#if LLCP
using combit.Logging;
#endif


namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provides a data provider implementation for Azure Cosmos DB by wrapping queried items in an <see cref="ObjectDataProvider"/>.
    /// </summary>
    /// <remarks>
    /// The <see cref="CosmosDbDataProvider{T}"/> class connects to a Cosmos DB instance using the provided URI and token, queries the specified database
    /// and container using a SQL query, and internally wraps the returned items in an <see cref="ObjectDataProvider"/>.
    /// </remarks>
    /// <example>
    /// The following example demonstrates how to query Cosmos DB using the <see cref="CosmosDbDataProvider{T}"/> and export the resulting report to PDF:
    /// <code language="csharp">
    /// // Create a CosmosDbDataProvider for a specific keyspace.
    /// CosmosDbDataProvider&lt;MyItemType&gt; provider = new CosmosDbDataProvider&lt;MyItemType&gt;(
    ///     "https://your-cosmosdb-uri", 
    ///     "your-access-token", 
    ///     "your-database", 
    ///     "your-container", 
    ///     "SELECT * FROM c");
    /// 
    /// // Create and configure the List &amp; Label reporting engine.
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
    public sealed class CosmosDbDataProvider<T> : IDataProvider, ISupportsLogger
    {
        private ILlLogger _logger;
        private Database _databaseID;
        private string _sqlQueryText = string.Empty;
        private ObjectDataProvider _objectDataProvider;
        private Container _containerID;

        /// <inheritdoc />
        bool IDataProvider.SupportsAnyBaseTable => ((IDataProvider)_objectDataProvider).SupportsAnyBaseTable;

        /// <inheritdoc />
        ReadOnlyCollection<ITable> IDataProvider.Tables => ((IDataProvider)_objectDataProvider).Tables;

        /// <inheritdoc />
        ReadOnlyCollection<ITableRelation> IDataProvider.Relations => ((IDataProvider)_objectDataProvider).Relations;

        /// <summary>
        /// Initializes a new instance of the <see cref="CosmosDbDataProvider{T}"/> class.
        /// </summary>
        /// <param name="uri">The URI of the Cosmos DB account.</param>
        /// <param name="token">The access token for the Cosmos DB account.</param>
        /// <param name="database">The name of the database to query.</param>
        /// <param name="container">The name of the container to query.</param>
        /// <param name="sqlQuery">
        /// An optional SQL query to execute. If not provided, the default query "SELECT * FROM c" is used.
        /// </param>
        /// <exception cref="CosmosException">
        /// Thrown when an error occurs during the query execution.
        /// </exception>
        public CosmosDbDataProvider(string uri, string token, string database, string container, string sqlQuery = null)
        {
            try
            {
                CosmosClient cosmosClient = new CosmosClient(uri, token, new CosmosClientOptions() { ApplicationName = null });
                _databaseID = cosmosClient.GetDatabase(database);
                _containerID = _databaseID.GetContainer(container);
                _sqlQueryText = sqlQuery;
                _objectDataProvider = QueryItems(_sqlQueryText);
            }
            catch (CosmosException ex)
            {
                Logger.Error(LogCategory.DataProvider, "Error while querying cosmosDB: {0}.", ex.Message);
                throw;
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Logger.Error(LogCategory.DataProvider, "Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
        }

        /// <summary>
        /// Gets the logger used by the data provider.
        /// </summary>
        internal ILlLogger Logger { get { return _logger ?? LoggingHelper.LlCoreDebugOutputLogger; } }

        /// <summary>
        /// Sets the logger for the data provider.
        /// </summary>
        /// <param name="logger">The logger instance to use.</param>
        /// <param name="overrideExisting">
        /// If set to <c>true</c>, any existing logger will be overridden; otherwise, the current logger remains unchanged.
        /// </param>
        void ISupportsLogger.SetLogger(ILlLogger logger, bool overrideExisting)
        {
            if (_logger == null || overrideExisting)
            {
                _logger = logger;
            }
        }

        /// <summary>
        /// Executes the specified SQL query against the Cosmos DB container and wraps the returned items in an <see cref="ObjectDataProvider"/>.
        /// </summary>
        /// <param name="sqlQuery">
        /// The SQL query to execute. If <c>null</c> or empty, the default query "SELECT * FROM c" is used.
        /// </param>
        /// <returns>An <see cref="ObjectDataProvider"/> containing the queried items.</returns>
        private ObjectDataProvider QueryItems(string sqlQuery)
        {
            string sqlQueryText = string.IsNullOrEmpty(sqlQuery) ? "SELECT * FROM c" : sqlQuery;
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            FeedIterator<T> queryResultSetIterator = _containerID.GetItemQueryIterator<T>(queryDefinition);

            var task = queryResultSetIterator.ReadNextAsync();
            var results = task.Result;

            ObjectDataProvider objectDataProvider = new ObjectDataProvider(results);
            return objectDataProvider;
        }

        /// <inheritdoc />
        ITable IDataProvider.GetTable(string tableName)
        {
            return ((IDataProvider)_objectDataProvider).GetTable(tableName);
        }

        /// <inheritdoc />
        ITableRelation IDataProvider.GetRelation(string relationName)
        {
            return ((IDataProvider)_objectDataProvider).GetRelation(relationName);
        }
    }
}