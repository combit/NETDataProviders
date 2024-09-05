using System.Collections.Generic;
using NJsonSchema;

namespace combit.Reporting.DataProviders
{
    public class JsonSchemaTable : JsonTable
    {
        private JsonSchema _schemaData;
        private ITableRow _cachedSchemaDataRow = null;

        public JsonSchemaTable(string tableName, JsonData data, SchemaAwareJsonDataProvider provider, JsonSchema schemaData = null) : base(tableName, data, provider)
        {
            _schemaData = schemaData;
        }

        public override ITableRow SchemaRow
        {
            get
            {
                //get first real row as schema row if data is present
                if (Data != null && _cachedSchemaDataRow == null)
                {
                    var enumerator = GetEnumerator();
                    if (enumerator.MoveNext())
                    {
                        _cachedSchemaDataRow = enumerator.Current;
                    }
                }

                if (_cachedSchemaDataRow != null)
                {
                    return _cachedSchemaDataRow;
                }

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
