using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using NJsonSchema;

namespace combit.Reporting.DataProviders
{
    internal class JsonSchemaTableRow : JsonTableRow
    {
        private JsonSchema _schemaData;

        public JsonSchemaTableRow(JsonData data, JsonSchema schemaData, string tableName, SchemaAwareJsonDataProvider provider) : base(data, tableName, provider)
        {
            _schemaData = schemaData;
        }

        #region ITableRow Members

        public override ReadOnlyCollection<ITableColumn> Columns
        {
            get
            {
                if (TableColumns != null)
                    return TableColumns.AsReadOnly();

                if (Data == null)
                {
                    TableColumns = new List<ITableColumn>();
                    TableColumns.AddRange(JsonSchemaOnlyTableRow.GetColumnsFromJsonSchema(_schemaData, Provider));
                    return TableColumns.AsReadOnly();
                }
                else
                {
                    return base.Columns;
                }
            }
        }

        protected override IDictionary GetDictionaryFromJsonData(JsonData data)
        {
            return _schemaData.ActualProperties.Keys.ToDictionary(x => x);
        }

        protected override JsonTableColumn GetColumnFromData(JsonData data, string key, string columnName)
        {
            if (!data.ContainsKey(key))
            {
                //add empty column from schema
                var item = _schemaData.ActualProperties[key];
                if (!item.IsArray)
                {
                    return JsonSchemaOnlyTableRow.ColumnFromSchemaData(columnName, item);
                }
            }
            else
            {
                return base.GetColumnFromData(data, key, columnName);
            }
            return null;
        }

        #endregion
    }
}
