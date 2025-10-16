using NJsonSchema;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.ComponentModel;


#if LLCP
using combit.Logging;
#endif


namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provides JSON data handling with schema awareness, allowing for validation and structured processing.
    /// Inherits from <see cref="JsonDataProvider"/> and extends its functionality to work with JSON schema definitions.
    /// </summary>
    public class SchemaAwareJsonDataProvider : JsonDataProvider
    {
        private string _schemaLocation;
        private JsonSchema _schema;
        private Dictionary<string, string> _tableRefPaths = new Dictionary<string, string>();

        /// <summary>
        /// Wraps the internal alias dictionary from the base class.
        /// </summary>
        internal new Dictionary<string, string> AliasDictionary => base.AliasDictionary;

        /// <summary>
        /// Wraps the internal logger instance from the base class.
        /// </summary>
        internal new ILlLogger Logger => base.Logger;

        /// <summary>
        /// Gets or sets the <see cref="NetworkFileProvider"/> used for resolving the <see cref="Schema"/>.
        /// If not set, <see cref="JsonDataProviderOptions.FileProvider"/> will be used as a fallback.
        /// </summary>
        public NetworkFileProvider SchemaFileProvider { get; set; }

        /// <summary>
        /// Gets or sets the file path or URL that points to a JSON schema definition.
        /// The schema is resolved using the provided <see cref="SchemaFileProvider"/>.
        /// If <see cref="SchemaFileProvider"/> is not set, the default <see cref="JsonDataProviderOptions.FileProvider"/> will be used.
        /// <para>
        /// - Properties must be directly provided under the "properties" key (single schema format).  
        /// - "AnyOf" schemas are not currently supported.  
        /// - The <see cref="JsonDataProvider.RootTableName"/> is set to the schema title.  
        /// - The <see cref="JsonDataProvider.ArrayValueName"/> is set to the array item name if the data is an array.
        /// </para>
        /// </summary>
        /// <seealso cref="SchemaFileProvider"/>
        public string Schema
        {
            get => _schemaLocation;
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
        /// Gets or sets a value indicating whether to generate a unique table for each subschema.
        /// If <c>false</c>, a single table will be generated for each subschema, even if used multiple times in the schema.
        /// </summary>
        public bool UseUniqueTables { get; set; } = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaAwareJsonDataProvider"/> class using a JSON string.
        /// </summary>
        /// <param name="json">The JSON string to parse.</param>
        public SchemaAwareJsonDataProvider(string json) : base(json)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaAwareJsonDataProvider"/> class using a <see cref="TextReader"/>.
        /// </summary>
        /// <param name="reader">The text reader containing JSON data.</param>
        public SchemaAwareJsonDataProvider(TextReader reader) : base(reader)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SchemaAwareJsonDataProvider"/> class using a file path or URL.
        /// </summary>
        /// <param name="filePathOrUrl">The file path or URL to the JSON data.</param>
        /// <param name="options">The JSON data provider options.</param>
        public SchemaAwareJsonDataProvider(string filePathOrUrl, JsonDataProviderOptions options) : base(filePathOrUrl, options)
        {
        }

        /// <inheritdoc />
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        protected override void InitDom()
        {
            if (Schema == null)
            {
                base.InitDom();
                return;
            }

            if (Data.IsObject)
            {
                BuildDomFromSchema(Data, RootTableName, _schema.ActualSchema);
            }
            else if (Data.IsArray && Data.Count != 0)
            {
                JsonData wrapper = new JsonData();
                wrapper[ArrayValueName] = Data;
                var schemaWrapper = new JsonSchema();
                schemaWrapper.Properties[ArrayValueName] = new JsonSchemaProperty { Item = _schema.Item?.ActualSchema, Type = _schema.Type };
                BuildDomFromSchema(wrapper, RootTableName, schemaWrapper);
            }
            else
            {
                throw new ListLabelException("JSON data needs to be an object or non-empty array on the root level.");
            }
        }

        /// <summary>
        /// Determines whether a given JSON schema can be flattened.
        /// </summary>
        /// <param name="schema">The JSON schema to check.</param>
        /// <returns><c>true</c> if the schema can be flattened; otherwise, <c>false</c>.</returns>
        internal bool IsFlattableR(JsonSchema schema)
        {
            if (!FlattenStructure)
                return false;

            if (schema.Type == JsonObjectType.Array)
            {
                foreach (var item in schema.ActualProperties)
                {
                    if (!IsFlattableR(item.Value.ActualSchema))
                        return false;
                }
            }

            foreach (var sData in schema.ActualProperties)
            {
                if (sData.Value.Type != JsonObjectType.Null)
                {
                    if (sData.Value.Type == JsonObjectType.Array)
                        return false;
                    else if (sData.Value.Type == JsonObjectType.Object && !IsFlattableR(sData.Value.ActualSchema))
                        return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Builds the data object model (DOM) based on a JSON schema.
        /// </summary>
        /// <param name="data">The JSON data.</param>
        /// <param name="tableName">The name of the table.</param>
        /// <param name="schema">The JSON schema definition.</param>
        private void BuildDomFromSchema(JsonData data, string tableName, JsonSchema schema = null)
        {
            // first, create a new table instance for the data
            JsonSchemaTable table = new JsonSchemaTable(tableName, data, this, schema.ActualSchema);
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

            // enumerate all schema
            foreach (var property in schema.ActualProperties)
            {
                string propertyName = property.Key;
                JsonObjectType propertyType = property.Value.ActualSchema.Type;

                JsonData objectData = null;
                if (objectToParse != null && objectToParse.ContainsKey(propertyName))
                    objectData = objectToParse[propertyName];

                if (propertyType.HasFlag(JsonObjectType.Object))
                {
                    if (IsFlattableR(property.Value.ActualSchema))
                    {
                        continue;
                    }
                    else
                    {
                        string newTableName = GetUniqueTableName(propertyName);
                        JsonTableRelation relation = new JsonTableRelation(GetUniqueRelationName(tableName, newTableName), tableName, newTableName);
                        RelationList.Add(relation);
                        BuildDomFromSchema(objectData, newTableName, schema.ActualProperties[propertyName].ActualSchema);
                    }
                }
                // schema.ActualProperties[propertyName].Item may be null if schema has no other definitions for the array ("Item": { "type": "array" }) => we wont add this array since it will always be an empty array with no columns
                else if (propertyType.HasFlag(JsonObjectType.Array) && schema.ActualProperties[propertyName].Item != null || schema.Properties[propertyName].Items.Any())
                {
                    if (UseUniqueTables)
                    {
                        // need to add a table anyway, either for a true object or just a fake table using "ArrayValue" as field name
                        string newTableName = GetUniqueTableName(propertyName);
                        JsonTableRelation relation = new JsonTableRelation(GetUniqueRelationName(tableName, newTableName), tableName, newTableName);
                        RelationList.Add(relation);

                        // see if there is an object underneath
                        var item = schema.ActualProperties[propertyName].Item?.ActualSchema ?? schema.ActualProperties[propertyName].Items.First().ActualSchema;
                        BuildDomFromSchema(objectData, newTableName, item);
                    }
                    else
                    {
                        var item = schema.ActualProperties[propertyName].Item?.ActualSchema ?? schema.ActualProperties[propertyName].Items.First().ActualSchema;
                        var schemaPath = JsonPathUtilities.GetJsonPath(_schema, item);

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
                            BuildDomFromSchema(objectData, newTableName, item);
                        }
                    }
                }
            }
        }
    }
}