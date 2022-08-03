using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using NJsonSchema;

namespace combit.Reporting.DataProviders
{
    internal class JsonSchemaOnlyTableRow : ITableRow
    {
        JsonSchema _data;
        SchemaAwareJsonDataProvider _provider;

        public JsonSchemaOnlyTableRow(JsonSchema data, string tableName, SchemaAwareJsonDataProvider provider)
        {
            _data = data;
            TableName = tableName;
            _provider = provider;
        }

        #region ITableRow Members

        public bool SupportsGetParentRow => false;
        public string TableName { get; private set; }

        List<ITableColumn> _columns;

        internal static JsonTableColumn ColumnFromSchemaData(string columnName, JsonSchemaProperty data)
        {
            //https://json-schema.org/draft/2020-12/json-schema-core.html#rfc.section.4.2.1
            if (data == null || data.Type == JsonObjectType.Null)
            {
                // best guess...
                return new JsonTableColumn(columnName, typeof(string), LlConstants.NullValue);
            }

            if (data.Type == JsonObjectType.Boolean)
            {
                return new JsonTableColumn(columnName, typeof(bool), null);
            }

            //https://json-schema.org/draft/2020-12/json-schema-validation.html#rfc.section.7.3.1
            if (data.Type == JsonObjectType.String && (data.Format == "date-time" || data.Format == "date" || data.Format == "time"))
            {
                return new JsonTableColumn(columnName, typeof(DateTime), null);
            }

            if (data.Type == JsonObjectType.String)
            {
                return new JsonTableColumn(columnName, typeof(string), LlConstants.NullValue);
            }

            //An arbitrary-precision, base-10 decimal number value, from the JSON "number" value
            if (data.Type == JsonObjectType.Number)
            {
                return new JsonTableColumn(columnName, typeof(double), null);
            }

            if (data.Type == JsonObjectType.Integer)
            {
                return new JsonTableColumn(columnName, typeof(int), null);
            }

            return null;
        }

        public ReadOnlyCollection<ITableColumn> Columns
        {
            get
            {
                if (_columns != null)
                    return _columns.AsReadOnly();

                _columns = new List<ITableColumn>();
                _columns.AddRange(GetColumnsFromJsonSchema(_data, _provider));
                return _columns.AsReadOnly();
            }
        }

        internal static IEnumerable<ITableColumn> GetColumnsFromJsonSchema(JsonSchema data, JsonDataProvider provider, string prefix = "")
        {
            var columns = new List<ITableColumn>();

            // this is an unnamed array like "array": [1,2,3]
            string columnPrefix = !string.IsNullOrEmpty(prefix) ? prefix + "." : "";
            if (!data.IsObject)
            {
                columns.Add(ColumnFromSchemaData($"{columnPrefix}{provider.ArrayValueName}", null));
                return columns.AsReadOnly();
            }

            foreach (var item in data.ActualProperties)
            {
                if (item.Value != null && item.Value.IsObject)
                {
                    foreach (var child in item.Value.Properties)
                    {
                        string columnName = $"{columnPrefix}{item.Key}.{child.Key}";
                        if (child.Value != null && child.Value.IsObject)
                        {
                            columns.AddRange(GetColumnsFromJsonSchema(child.Value, provider, columnName));
                            continue;
                        }
                        JsonTableColumn column = ColumnFromSchemaData(columnName, child.Value);
                        if (column != null)
                            columns.Add(column);
                    }
                }
                else
                {
                    JsonTableColumn column = ColumnFromSchemaData($"{columnPrefix}{item.Key}", item.Value);
                    if (column != null)
                        columns.Add(column);
                }
            }
            return columns;
        }

        public ITable GetChildTable(ITableRelation relation)
        {
            string tableName;
            if (_data.ActualProperties.ContainsKey(relation.ChildTableName))
            {
                return new JsonSchemaTable(relation.ChildTableName, null, _provider, _data.ActualProperties[relation.ChildTableName]);
            }
            else if (_provider.AliasDictionary.TryGetValue(relation.ChildTableName, out tableName))
            {
                return new JsonSchemaTable(relation.ChildTableName, null, _provider, _data.ActualProperties[tableName]);
            }
            else
            {
                _provider.Logger.Warn(LogCategory.DataProvider, "JSON: Property {0} not found in schema.", new object[] { relation.ChildTableName });
                return null;
            }
        }

        public ITableRow GetParentRow(ITableRelation relation)
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}
