using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace combit.Reporting.DataProviders
{
    public sealed class CosmosDbDataProvider<T> : IDataProvider, ISupportsLogger

    {
        private ILlLogger _logger;
        private Database _databaseID;

        private string _sqlQueryText = string.Empty;

        private ObjectDataProvider _objectDataProvider;

        private Container _containerID;

        public bool SupportsAnyBaseTable => ((IDataProvider)_objectDataProvider).SupportsAnyBaseTable;

        public ReadOnlyCollection<ITable> Tables => ((IDataProvider)_objectDataProvider).Tables;

        public ReadOnlyCollection<ITableRelation> Relations => ((IDataProvider)_objectDataProvider).Relations;

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
        internal ILlLogger Logger { get { return _logger ?? LoggingHelper.DummyLogger; } }
        public void SetLogger(ILlLogger logger, bool overrideExisting)
        {
            if (_logger == null || overrideExisting)
            {
                _logger = logger;
            }
        }
       
        public ObjectDataProvider QueryItems(string sqlQuery)
        {
            string sqlQueryText = String.Empty;
            sqlQueryText = string.IsNullOrEmpty(sqlQuery) ? "SELECT * FROM c" : sqlQuery;
            
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            FeedIterator<T> queryResultSetIterator = _containerID.GetItemQueryIterator<T>(queryDefinition);

            var task = queryResultSetIterator.ReadNextAsync();
            var results = task.Result;

            ObjectDataProvider objectDataProvider = new ObjectDataProvider(results);
            return objectDataProvider;

        }

        public ITable GetTable(string tableName)
        {
            return ((IDataProvider)_objectDataProvider).GetTable(tableName);
        }

        public ITableRelation GetRelation(string relationName)
        {
            return ((IDataProvider)_objectDataProvider).GetRelation(relationName);
        }
    }
}