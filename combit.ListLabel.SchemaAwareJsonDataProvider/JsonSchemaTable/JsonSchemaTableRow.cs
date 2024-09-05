using NJsonSchema;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace combit.Reporting.DataProviders
{
    internal class JsonSchemaTableRow : JsonTableRow
    {
        private JsonSchema _schemaData;
        private SchemaAwareJsonDataProvider _dataProvider;

        public JsonSchemaTableRow(JsonData data, JsonSchema schemaData, string tableName, SchemaAwareJsonDataProvider provider) : base(data, tableName, provider)
        {
            _schemaData = schemaData;
            _dataProvider = provider;
        }

        #region ITableRow Members

        public override ReadOnlyCollection<ITableColumn> Columns
        {
            get
            {
                if (TableColumns != null)
                    return TableColumns.AsReadOnly();

                TableColumns = new List<ITableColumn>();
                IEnumerable<ITableColumn> schemaColumns = JsonSchemaOnlyTableRow.GetColumnsFromJsonSchema(_schemaData, _dataProvider);
                if (Data == null)
                {
                    TableColumns.AddRange(schemaColumns);
                    return TableColumns.AsReadOnly();
                }
                else
                {
                    TableColumns.AddRange(GetColumnsFromJsonData(Data));
                    // add schema only columns for columns that do not exist on json data
                    TableColumns.AddRange(schemaColumns.Where(c => !TableColumns.Any(tc => tc.ColumnName == c.ColumnName)));
                    return TableColumns.AsReadOnly();
                }
            }
        }

        protected override IDictionary GetDictionaryFromJsonData(JsonData data, string prefix)
        {
            var schema = _schemaData.ActualProperties;
            if (!string.IsNullOrEmpty(prefix))
            {
                var keys = prefix.Split('.');
                foreach (var key in keys)
                {
                    schema = schema[key].ActualProperties;
                }
            }

            return schema.Keys.ToDictionary(x => x);
        }

        JsonSchemaProperty FindSchemaItemDeep(JsonSchema jsonSchema, string key)
        {
            if (jsonSchema.ActualProperties.ContainsKey(key))
            {
                return jsonSchema.ActualProperties[key];
            }
            else if (!key.Contains("."))
            {
                //deep search cannot be performed
                return null;
            }

            //get the other keys and re-call the function on the first parent with its childs
            string[] subItems = key.Split('.');
            if (jsonSchema.ActualProperties.ContainsKey(subItems[0]))
            {
                return FindSchemaItemDeep(jsonSchema.ActualProperties[subItems[0]], string.Join(".", subItems.Skip(1).ToArray()));
            }

            return null;
        }

        protected override JsonTableColumn GetColumnFromData(JsonData data, string key, string columnName)
        {
            var item = FindSchemaItemDeep(_schemaData, columnName);
            if (item == null)
            {
                return base.GetColumnFromData(data, key, columnName);
            }

            if (!data.ContainsKey(key))
            {
                //add empty column from schema
                if (!item.IsArray)
                {
                    return JsonSchemaOnlyTableRow.ColumnFromSchemaData(columnName, item);
                }
            }
            else
            {
                var column = base.GetColumnFromData(data, key, columnName);
                if (!item.IsArray && column != null)
                {
                    //use DataType from schemaColumn for the actual column
                    var schemaColumn = JsonSchemaOnlyTableRow.ColumnFromSchemaData(columnName, item);
                    if (schemaColumn == null && column.Content is string missingSchemaStr && missingSchemaStr == LlConstants.NullValue)
                    {
                        //Ignore columns that are being added as NULL if no schema column could be found with the exact name (nested object is null and interpreted as standalone column instead of table)
                        return null;
                    }
                    else if (column.Content is string str && str == LlConstants.NullValue)
                    {
                        //special case if column is string, but type should be double and content should be null instead of NullValue
                        if (schemaColumn != null)
                            return new JsonTableColumn(columnName, schemaColumn.DataType, schemaColumn.Content);
                        else
                            return null;
                    }
                    else if (schemaColumn != null && schemaColumn.DataType == typeof(DateTime) && column.Content is string dateStr)
                    {
                        //if schema is date but json data provider could not parse value using ISO => try to parse generous as DateTime
                        if (DateTime.TryParse(dateStr, out var date))
                        {
                            return new JsonTableColumn(columnName, schemaColumn.DataType, date);
                        }
                        else
                        {
                            //if the value could not be parsed just add data as schema since specified date in json is broken
                            _dataProvider.Logger.Debug(LogCategory.DataProvider, $"JSON: Column {column.ColumnName} is of type DateTime but value \"{dateStr}\" could not be parsed to a DateTime value and will be registered as null.");
                            return new JsonTableColumn(columnName, schemaColumn.DataType, schemaColumn.Content);
                        }
                    }
                    else
                    {
                        if (schemaColumn != null)
                            return new JsonTableColumn(columnName, schemaColumn.DataType, column.Content);
                        else
                            return null;
                    }
                }
                else
                {
                    return column;
                }
            }
            return null;
        }

        public override ITable GetChildTable(ITableRelation relation)
        {
            if (Data.IsArray)
            {
                return new JsonSchemaTable(relation.ChildTableName, Data, _dataProvider, _schemaData);
            }
            else if (Data.ContainsKey(relation.ChildTableName))
            {
                var childSchema = _schemaData.ActualProperties[relation.ChildTableName].ActualSchema;
                var arrayItem = childSchema.Item.ActualSchema ?? childSchema.Items.FirstOrDefault()?.ActualSchema;
                return new JsonSchemaTable(relation.ChildTableName, Data[relation.ChildTableName], _dataProvider, childSchema.Type == JsonObjectType.Array ? arrayItem : childSchema);
            }
            else if (_dataProvider.AliasDictionary.TryGetValue(relation.ChildTableName, out string tableName))
            {
                var childSchema = _schemaData.ActualProperties[tableName].ActualSchema;
                var arrayItem = childSchema.Item.ActualSchema ?? childSchema.Items.FirstOrDefault()?.ActualSchema;
                return new JsonSchemaTable(relation.ChildTableName, Data[tableName], _dataProvider, childSchema.Type == JsonObjectType.Array ? arrayItem : childSchema);
            }
            else
            {
                return new JsonSchemaTable(relation.ChildTableName, null, _dataProvider, _schemaData);
            }
        }

        #endregion
    }
}
