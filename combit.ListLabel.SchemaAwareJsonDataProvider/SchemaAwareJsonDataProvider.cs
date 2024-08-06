using NJsonSchema;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace combit.Reporting.DataProviders
{
    public class SchemaAwareJsonDataProvider : JsonDataProvider
    {
        private string _schemaLocation;
        private JsonSchema _schema;
        private Dictionary<string, string> _tableRefPaths = new Dictionary<string, string>();
        //wrap internals
        internal new Dictionary<string, string> AliasDictionary => base.AliasDictionary;
        internal new ILlLogger Logger => base.Logger;

        /// <summary>
        /// The <see cref="NetworkFileProvider"/> that should be used for resolving the <see cref="Schema"/>.
        /// If this value is not provided, <see cref="JsonDataProviderOptions.FileProvider"/> will be used as a fallback.
        /// </summary>
        public NetworkFileProvider SchemaFileProvider { get; set; }

        /// <summary>
        /// The file- or url that points to a json schema definition (https://datatracker.ietf.org/doc/html/draft-bhutton-json-schema-00) which will be resolved by the provided <see cref="SchemaFileProvider"/>. If <see cref="SchemaFileProvider"/> is not set, the default <see cref="JsonDataProviderOptions.FileProvider"/> will be used.
        /// Properties must directly be provided under the properties-key (first single schema).
        /// AnyOf-Schema is currently not supported.
        /// RootTableName will be set to the schema title.
        /// ArrayValueName will be set to the array item name if data is an array.
        /// </summary>
        /// <seealso cref="SchemaFileProvider"/>
        public string Schema
        {
            get { return _schemaLocation; }
            set
            {
                if (_schemaLocation == value)
                    return;

                _schemaLocation = value;
                var fileProvider = SchemaFileProvider ?? Options?.FileProvider ?? new NetworkFileProvider();
                _schema = JsonSchema.FromJsonAsync(fileProvider.ReadAsString(_schemaLocation)).Result;

                RootTableName = _schema.Title ?? RootTableName;
                if (Data?.IsArray == true)
                {
                    ArrayValueName = _schema.Definitions?.FirstOrDefault().Key ?? ArrayValueName;
                }
            }
        }

        /// <summary>
        /// Generate a unqiue table for each subschema.
        /// If false, only one table will be generated for each subschema ($ref) which might be used multiple times in the schema.
        /// </summary>
        /// <param name="json"></param>
        public bool UseUniqueTables { get; set; } = true;

        public SchemaAwareJsonDataProvider(string json) : base(json)
        {
        }

        public SchemaAwareJsonDataProvider(TextReader reader) : base(reader)
        {
        }

        public SchemaAwareJsonDataProvider(string filePathOrUrl, JsonDataProviderOptions options) : base(filePathOrUrl, options)
        {
        }

        protected override void InitDom()
        {
            if (Schema == null) //dont use string.IsNullOrEmpty since string.Empty may be valid, if custom FileProvider is used
            {
                base.InitDom();
                return;
            }

            if (Data.IsObject)
            {
                BuildDomFromSchema(Data, RootTableName, _schema.ActualSchema);
            }
            else
            {
                if (Data.IsArray && Data.Count != 0)
                {
                    JsonData wrapper = new JsonData();
                    wrapper[ArrayValueName] = Data;
                    var schemaWrapper = new JsonSchema();
                    schemaWrapper.Properties[ArrayValueName] = new JsonSchemaProperty { Item = _schema.Item.ActualSchema, Type = _schema.Type };
                    BuildDomFromSchema(wrapper, RootTableName, schemaWrapper);
                }
                else
                    throw new LL_BadDatabaseStructure_Exception("JSON data needs to be an object or non-empty array on the root level.");
            }
        }

        internal bool IsFlattableR(JsonSchemaProperty schema)
        {
            if (!FlattenStructure)
                return false;

            if (schema.Type == JsonObjectType.Array)
            {
                foreach (var item in schema.ActualProperties)
                {
                    if (!IsFlattableR(item.Value))
                        return false;
                }
            }

            foreach (KeyValuePair<string, JsonSchemaProperty> sData in schema.ActualProperties)
            {
                if (sData.Value.Type != JsonObjectType.Null)
                {
                    if (sData.Value.Type == JsonObjectType.Array)
                        return false;
                    else if (sData.Value.Type == JsonObjectType.Object)
                    {
                        if (!IsFlattableR(sData.Value))
                            return false;
                    }
                }
            }

            return true;
        }

        private void BuildDomFromSchema(JsonData data, string tableName, JsonSchema schema = null)
        {
            // first, create a new table instance for the data
            JsonSchemaTable table = new JsonSchemaTable(tableName, data, this, schema);
            TableList.Add(table);

            JsonData objectToParse;

            if (data != null && data.IsArray)
            {
                objectToParse = data.Count > 0 ? data[0] : null;
            }
            else
            {
                objectToParse = data;
            }

            //enumerate all schema
            foreach (var property in schema.Properties)
            {
                string propertyName = property.Key;
                JsonData objectData = null;
                if (objectToParse != null && objectToParse.ContainsKey(propertyName))
                    objectData = objectToParse[propertyName];

                if (property.Value.ActualSchema.Type == JsonObjectType.Object)
                {
                    if (IsFlattableR(property.Value))
                    {
                        continue;
                    }
                    else
                    {
                        string newTableName = GetUniqueTableName(propertyName);
                        JsonTableRelation relation = new JsonTableRelation(GetUniqueRelationName(tableName, newTableName), tableName, newTableName);
                        RelationList.Add(relation);
                        BuildDomFromSchema(objectData, newTableName, schema.Properties[propertyName].ActualSchema);
                    }
                }
                // schema.Properties[propertyName].Item may be null if schema has no other definitions for the array ("Item": { "type": "array" }) => we wont add this array since it will always be an empty array with no columns
                else if (property.Value.Type == JsonObjectType.Array && schema.Properties[propertyName].Item != null)
                {
                    if (UseUniqueTables)
                    {
                        // need to add a table anyway, either for a true object or just a fake table using "ArrayValue" as field name
                        string newTableName = GetUniqueTableName(propertyName);
                        JsonTableRelation relation = new JsonTableRelation(GetUniqueRelationName(tableName, newTableName), tableName, newTableName);
                        RelationList.Add(relation);

                        // see if there is an object underneath
                        BuildDomFromSchema(objectData, newTableName, schema.Properties[propertyName].Item);
                    }
                    else
                    {
                        var schemaPath = JsonPathUtilities.GetJsonPath(_schema, schema.Properties[propertyName].Item.ActualSchema);

                        if (_tableRefPaths.TryGetValue(propertyName, out string existingSchemaPath) && schemaPath == existingSchemaPath)
                        {
                            // The same schema with the same table name was already added
                            JsonTableRelation relation = new JsonTableRelation(GetUniqueRelationName(tableName, propertyName), tableName, propertyName);
                            RelationList.Add(relation);
                        }
                        else
                        {
                            // need to add a table anyway, either for a true object or just a fake table using "ArrayValue" as field name
                            string newTableName = GetUniqueTableName(propertyName);
                            JsonTableRelation relation = new JsonTableRelation(GetUniqueRelationName(tableName, newTableName), tableName, newTableName);
                            RelationList.Add(relation);

                            _tableRefPaths.Add(newTableName, schemaPath);

                            // see if there is an object underneath
                            BuildDomFromSchema(objectData, newTableName, schema.Properties[propertyName].Item.ActualSchema);
                        }
                    }
                }
            }
        }
    }
}