using System.Collections.Generic;
using NJsonSchema;

namespace combit.Reporting.DataProviders
{
    public class JsonSchemaTable : JsonTable
    {
        private JsonSchema _schemaData;

        public JsonSchemaTable(string tableName, JsonData data, SchemaAwareJsonDataProvider provider, JsonSchema schemaData = null) : base(tableName, data, provider)
        {
            _schemaData = schemaData;
        }

        public override ITableRow SchemaRow
        {
            get
            {
                return new JsonSchemaOnlyTableRow(_schemaData, TableName, Provider as SchemaAwareJsonDataProvider);
            }
        }

        public override IEnumerator<ITableRow> GetEnumerator()
        {
            if (Data == null)
                return null;
            return new JsonSchemaEnumerator(Data, _schemaData, this, Provider as SchemaAwareJsonDataProvider);
        }
    }
}
