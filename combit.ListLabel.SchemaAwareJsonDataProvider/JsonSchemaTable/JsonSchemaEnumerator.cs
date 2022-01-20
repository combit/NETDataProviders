using NJsonSchema;

namespace combit.Reporting.DataProviders
{
    internal class JsonSchemaEnumerator : JsonEnumerator
    {
        private JsonSchema _schemaData;

        public JsonSchemaEnumerator(JsonData data, JsonSchema schemaData, JsonSchemaTable table, SchemaAwareJsonDataProvider provider) : base(data, table, provider)
        {
            _schemaData = schemaData;
        }

        public override ITableRow Current
        {
            get
            {
                if (CachedCurrentRow == null)  // value of Current should not be constructed again until the next call to Move()
                {
                    if (Enumerator != null)  // has json object child items?
                    {
                        // this row is an array in json ( -> create rows for the objects in the array)
                        CachedCurrentRow = new JsonSchemaTableRow(Enumerator.Current as JsonData, _schemaData, Table.TableName, Provider as SchemaAwareJsonDataProvider);
                    }
                    else
                    {
                        // this row is an object in json (-> return self, json properties become the columns)
                        CachedCurrentRow = new JsonSchemaTableRow(Data, _schemaData, Table.TableName, Provider as SchemaAwareJsonDataProvider);
                    }
                }

                return CachedCurrentRow;
            }
        }
    }
}
