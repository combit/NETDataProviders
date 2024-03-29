﻿/**********************************************************************
 * Copyright (C) 2016 by Taste IT Consulting ("TIC") -                *
 * www.taste-consulting.de and other contributors as listed           *
 * below.  All Rights Reserved.                                       *
 *                                                                    *
 *  Software is distributed on an "AS IS", WITHOUT WARRANTY OF ANY    *
 *  KIND, either express or implied.                                  *
 *  See the Microsoft Public License (Ms-PL) for more details.        *
 *  You should have received a copy of the Microsoft Public License   *
 *  in <license.txt> along with this software. If not, see            *
 *  <http://www.microsoft.com/en-us/openness/licenses.aspx#MPL>.      *
 *                                                                    *
 *  Contributors:                                                     *
 *                                                                    *
 **********************************************************************/
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;
using System.CodeDom.Compiler;
using System.IO;
using System.Drawing;
using combit.Reporting;
using combit.Reporting.DataProviders;
using Microsoft.Win32;

namespace TasteITConsulting.Reporting
{
    // An OpenEdge data provider 
    public class OpenEdgeDataProvider : IDataProvider, ICanHandleUsedIdentifiers, ISupportsSetParentObject, ISupportsLogger, IDisposable, IRequireCollectedReportParameters
    {
        private List<ITable>           _tables        = null;
        private List<ITableRelation>   _relations     = null;
        private List<OpenEdgeView>     _views         = null;
        private List<ITable>           _viewtables    = null;
        private List<ITableRelation>   _viewrelations = null;       
		private ListLabel _LL = null;		
        private TempFileCollection     _tempfiles     = null;
        private OpenEdgeView _currentView = null;
        private Guid _instanceguid = Guid.NewGuid();


        //private string[] _usedRelations = null;
        public bool DesignMode { get; private set; } = true;
        public bool DebugMode  { get; set; } = false;
        public bool UseInvariantCulture { get; set; } = false;
        public string ViewName { get; set; } = "";
        public int MaxRows { get; set; } = 0;
        // Used internal as separator for foreignkey values 
        internal char ValueDelimiter { get; set; } = (char)1;

        private bool disposed = false;
        

        #region IDataProvider members
        public ReadOnlyCollection<ITableRelation> Relations
        {
            get
            {
                if (_currentView == null)
                    return _relations.AsReadOnly();
                else
                    return _viewrelations.AsReadOnly();
            }
        }

        public bool SupportsAnyBaseTable { get; set; } = true;

        public ReadOnlyCollection<ITable> Tables
        {
            get
            {
                if (_currentView == null)
                    return _tables.AsReadOnly();
                else
                    return _viewtables.AsReadOnly();
            }
        }

        public ITableRelation GetRelation(string relationName)
        {
            foreach (ITableRelation r in _relations)
            {
                if (string.Equals(r.RelationName, relationName, StringComparison.OrdinalIgnoreCase))
                    return r;
            }
            return null;
        }

        public ITable GetTable(string tableName)
        {
            // Since LL26: Whenever GetTable is called by LL we need to create a copy of the 
            // Master Table (Clone). The clone method is inside the OpenEdgeTable class.
            // GetTable() is no longer called internally. GetMasterTable() is used instead.
            // The reason for cloning a table is that there can be multiple instances of the same
            // table class i.e when we have multiple containers or recursive table relations.
            foreach (OpenEdgeTable masterTable in _tables)
            {
                if (string.Equals(masterTable.TableName, tableName, StringComparison.OrdinalIgnoreCase))
                    return masterTable.Clone();
            }
            return null;
        }

        internal ITable GetMasterTable(string tableName)
        {
            foreach (ITable masterTable in _tables)
            {
                if (string.Equals(masterTable.TableName, tableName, StringComparison.OrdinalIgnoreCase))
                    return masterTable;
            }
            return null;
        }

        internal OpenEdgeTable GetOpenEdgeTable(string openEdgeTableName)
        {
            foreach (OpenEdgeTable t in _tables)
            {
                if (string.Equals(t.OETableName, openEdgeTableName, StringComparison.OrdinalIgnoreCase))
                    return t;
            }
            return null;
        }

        public bool SetBaseQueryWhere (string tableName, string baseQueryWhere)
        {
            OpenEdgeTable Table = GetOpenEdgeTable(tableName);
            if (Table != null)
            {
                Table.BaseQueryWhere = baseQueryWhere;
                return true;
            }
            return false;
        }

        // 20170815 - LL22: Required for MasterMode AsVariables since there is currently no way to define sorting for the MasterTable
        //            (This will propably change in LL23)              
        public bool SetInitialSortBy(string tableName, string openEdgeSortDescription)
        {
            OpenEdgeTable Table = GetOpenEdgeTable(tableName);
            if (Table != null)
            {
                Table.InitialSortBy = openEdgeSortDescription;
                return true;
            }
            return false;
        }

        internal OpenEdgeTableRelation GetOpenEdgeFKRelation(string ForeignKeyIdentifier)
        {
            OpenEdgeTableRelation _relation;
            foreach (ITableRelation r in _relations)
            {
                _relation = r as OpenEdgeTableRelation;
                if (_relation.ForeignKeyIdentifier == ForeignKeyIdentifier)
                    return _relation;
            }
            return null;
        }

        internal OpenEdgeTableRelation GetOpenEdgeRelation(string RelationIdentifier)
        {
            OpenEdgeTableRelation _relation;
            foreach (ITableRelation r in _relations)
            {
                _relation = r as OpenEdgeTableRelation;
                if (_relation.RelationIdentifier == RelationIdentifier)
                    return _relation;
            }
            return null;
        }

        #endregion

        #region ICanHandleUsedIdentifiers members 
        public void SetUsedIdentifiers(ReadOnlyCollection<string> identifiers)
        {
            DebugOutput(String.Format("Provider - Used Identifiers: {0}", String.Join(",", identifiers)));
            if (identifiers.Count > 0)
                DesignMode = false;
            else
                DesignMode = true;
            // 20170316: The new helper has a cache and a second run with different second parameter returns the wrong result.
            //           As a workaround we use the objects for now since LL has a new SP
            UsedIdentifierHelper helper = new UsedIdentifierHelper(identifiers);
            UsedIdentifierHelper helper2 = new UsedIdentifierHelper(identifiers);

            foreach (OpenEdgeTable t in _tables)
            {
                ReadOnlyCollection<string> usedHere;
                // 20161229: LL 22.1.0.0 Helper has been changed by combit. GetJoinedIdentifiers() doesn't exist anymore.
                // 20170731: LL 23.Alpha First Parameter this.  
                // There is a new parameter for GetIdentifiersForTable() instead. 
                usedHere = helper.GetIdentifiersForTable(this,t.TableName,false);
                t.SetUsedIdentifiers(usedHere);
                //usedHere = helper.GetJoinedIdentifiersForTable(t.TableName);
                usedHere = helper2.GetIdentifiersForTable(this,t.TableName,true);
                t.SetJoinedIdentifiers(usedHere);
            }
            DebugOutput("Provider - Used Identifiers done");
        }
        #endregion

        #region ISupportsSetParentObject members
        public void SetParentObject (Object parent)
        {
            if (parent is ListLabel)
            {
                _LL = parent as ListLabel;                
            }
        }

        #endregion

        #region ISupportsLogger members
        private ILlLogger _logger = null;
        public void SetLogger(ILlLogger logger, bool overrideExisting)
        {
            if (_logger == null || overrideExisting)
                _logger = logger;
        }
        #endregion

        #region OpenEdge properties
        internal OpenEdgeServiceParameter ServiceParameter { get; private set; }
        public string ServiceName { get; set; }
        public IServiceAdapter ServiceAdapter { get; set; }
        public string DefinedTables
        {
            get
            {
                string list = "";
                string sep = "";
                foreach (ITable t in _tables)
                {
                    list += sep + t.TableName;
                    sep = ",";
                }
                return list;

            }
        }
        public string DefinedViews
        {
            get
            {
                string list = "";
                string sep = "";
                foreach (OpenEdgeView v in _views)
                {
                    list += sep + v.ViewName;
                    sep = ",";
                }
                return list;
            }
        }
        public string ViewTables (string ViewName )
        {
            foreach (OpenEdgeView v in _views)
            {
                if (string.Equals(v.ViewName, ViewName, StringComparison.OrdinalIgnoreCase))
                    return v.ViewTables;
            }
            return "";
        }

        // Unique Id to indentify context for the server 
        public string ClientId
        {
            get
            {
                return _instanceguid.ToString();
            }
        }

        // Since LL26
        // Sequence for Clones 
        private int TableInstanceIdSequence = 0;

        // Since LL26 the current table instance LL is working with
        internal OpenEdgeTable CurrentOpenEdgeTable { get; set; }

        #endregion

        private OpenEdgeServiceCatalogReader _schemaReader;

        // Constructor
        public OpenEdgeDataProvider()
        {
            _schemaReader    = new OpenEdgeServiceCatalogReader();
            _tables          = new List<ITable>();
            _relations       = new List<ITableRelation>();
            _views           = new List<OpenEdgeView>();
            _tempfiles       = new TempFileCollection();
            ServiceParameter = new OpenEdgeServiceParameter(this);
        }

        public void Initialize()
        {
            DeleteSchema();
            DesignMode = true;
            //_usedRelations = null;
            BuildSchema();
            _currentView = null;
            if (ViewName != "")
            {
                foreach (OpenEdgeView v in _views)
                {
                    if (v.ViewName.ToLower() == ViewName.ToLower())
                    {
                        _currentView = v;
                        break;
                    }
                }
            }
            if (_currentView != null)
            {
                string[] UsedTables = _currentView.ViewTables.Split(',');
                string[] UsedRelations = _currentView.ViewRelations.Split(',');
                _viewtables    = new List<ITable>();
                _viewrelations = new List<ITableRelation>(); 
                for (int i = 0; i < UsedTables.Length; i++)
                {
                    ITable Table = GetMasterTable(UsedTables[i]);
                    if (Table != null)
                        _viewtables.Add(Table);
                }
                for (int i = 0; i < UsedRelations.Length; i++)
                {
                    ITableRelation Relation = GetRelation(UsedRelations[i]);
                    if (Relation != null)
                        _viewrelations.Add(Relation);
                }
            }
        }
        
        private void DeleteSchema()
        {
            _tables.Clear();
            _relations.Clear();
        }

        private void BuildSchema()
        {
            OELongchar jsonParameter = new OELongchar();
            ServiceParameter.AssignDefaultServiceParameter();
            jsonParameter.Data = ServiceParameter.GetJson();
            _ = ServiceAdapter.GetSchema(ServiceName, jsonParameter, out OELongchar jsonSchema);
            _schemaReader.ReadSchema(this, ServiceName, jsonSchema.Data);
        }

        internal void AddTable(OpenEdgeTable Table)
        {
            _tables.Add(Table);
        }

        internal void AddRelation(OpenEdgeTableRelation Relation)
        {
            _relations.Add(Relation);
        }

        internal void AddView(OpenEdgeView View)
        {
            _views.Add(View);
        }

        public void SetServiceParameter(string name, object value)
        {
            ServiceParameter.SetParameterValue(name, value);
        }

        public void DebugOutput(string value)
        {
            if(_logger != null)
                _logger.Info(LogCategory.DataProvider, value);
        }

        internal string OEStringValue (object value)
        {
            DateTime dt;
            decimal de;

            string s;
            //if (value is DateTimeOffset)
            if (value is DateTime time)
            {
                dt = time;
                s = UseInvariantCulture ? dt.ToString("G", CultureInfo.InvariantCulture) : dt.ToString("G");
            }
            else if (value is decimal dec)
            {
                de = dec;
                s = UseInvariantCulture ? de.ToString("G", CultureInfo.InvariantCulture) : de.ToString("G");
            }
            else
            {
                s = value.ToString();
            }
            return s;
        }

        internal string GetTempFileName(string extension)
        {
            string filename = Path.GetTempFileName();            
            File.Move(filename, Path.ChangeExtension(filename, extension));
            filename = Path.ChangeExtension(filename, extension);
            _tempfiles.AddFile(filename, false);
            return filename;
        }

        internal string GetDefaultFileExtension(string mimeType)
        {            
            RegistryKey key = Registry.ClassesRoot.OpenSubKey(@"MIME\Database\Content Type\" + mimeType, false);
            object value = key?.GetValue("Extension", null);
            string extension = value != null ? value.ToString() : ".tmp";
            return extension;
        }
        // Event Handler for LL ReportParametersCollected 
        // since LL 26.002 also fired for the WebDesigner
        public void ReportParametersCollected(Dictionary<string, object> paramValues)
        {
            foreach (KeyValuePair<string, object> param in paramValues)
            {
                SetServiceParameter("LlReportParameter." + param.Key, param.Value);
            }
            // Notify service 
            NotifyClientEvent("ReportParametersCollected");

        }


        // Server/Service notification about things happening here 
        private void NotifyClientEvent (string eventName)
        {          
            OELongchar JsonServiceParameter = new OELongchar();
            ServiceParameter.AssignDefaultServiceParameter();
            JsonServiceParameter.Data = ServiceParameter.GetJson();
            OELongchar JsonDataRequest = new OELongchar();
            OpenEdgeClientEvent Event = new OpenEdgeClientEvent(ClientId, eventName);
            JsonDataRequest.Data = Event.GetJson();
            _ = ServiceAdapter.ClientEvent(ServiceName, JsonServiceParameter, JsonDataRequest, out OELongchar _ /*JsonDataResponse*/);
            return;
        }

        internal int NextTableInstanceId()
        {
            return TableInstanceIdSequence += 1;
        }

        // Dispose 
        public void Dispose ()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose (bool disposing)
        {
            if (disposed)
                return;

            // Free any other unmanaged objects here.
            if (disposing)
            {
                NotifyClientEvent("ClientDisposed");
            }
            disposed = true;
        }



        // finalizer 
        ~OpenEdgeDataProvider()
        {
            Dispose(false);
        }
    }

    // An OpenEdge Table 
    [SchemaRowUsageModeAttribute(SchemaRowUsageMode.Design | SchemaRowUsageMode.Print)]
    public class OpenEdgeTable : ITable, IEnumerable<ITableRow>, IAdvancedFiltering , ISupportNativeAggregateFunctions
    {
        private OpenEdgeTableRowEnumerator _enumerator = null;
        private List<string> _sortOrders = null;
        private string CurrentAdvancedFilter = "";
        private string CurrentFilter = "";
        private string CurrentSort = "";
        private RelationQuery ParentRelationQuery = null;        
        private ReadOnlyCollection<string> _thisTableJoinedIdentifiers;
        private ReadOnlyCollection<string> _thisTableUsedIdentifiers;
        private string UsedOETableColumns = "";
        private List<OpenEdgeTableRow> _tablerows = null;
        private int position = -1;
        internal List<OpenEdgeMultiValueFilter> CurrentMultiValueFilters;
      
        private List<OpenEdgeParentRowCache> _openEdgeParentRowCaches = new List<OpenEdgeParentRowCache>();

        #region ITable members
        public int Count
        {
            get
            {
                return _tablerows.Count;
            }
        }

        // If LL accesses the Rows property, it's time the get the records based on the current
        // filter and sort criteria.
        public IEnumerable<ITableRow> Rows
        {
            get
            {
                OpenQuery();
                return this as IEnumerable<ITableRow>;
            }
        }

        public IEnumerator<ITableRow> GetEnumerator()
        {
            return _enumerator;
        }

        #region IEnumerable Members
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        #endregion

        public ITableRow SchemaRow { get; set; }

        public ReadOnlyCollection<string> SortDescriptions
        {
            get
            {
                if (_sortOrders == null && SchemaRow != null)
                {
                    _sortOrders = new List<string>();

                    // SortOrderDisplayName | SortOrder OpenEdge Command | Column
                    // We don't support sorting on arrays. The backend creates an index for the sort order before writing json
                    // and arrays are not supported in indexes.
                    // We may send sort orders with the schema, but it's hard to decide what we want to add dynamically.
                    // There may be a rule that if the schema contains sortorders we use them otherwise we build a list here.
                    // It's also possible do add some flags to the Service Definition ...
                    // For now we offer sortings for all sortable columns 
                    foreach (ITableColumn column in SchemaRow.Columns)
                    {
                        OpenEdgeTableColumn c = column as OpenEdgeTableColumn;
                        string _columnName = c.OEColumnName;
                        if (c.Sortable == true)
                        {
                            // Old version of the data provider 
                            //_sortOrders.Add(String.Format("{0}| BY {1}|{0}", c.ColumnName, _columnName));
                            //_sortOrders.Add(String.Format("{0} (-)| BY {1} DESC|{0}", c.ColumnName, _columnName));
                            // List & Label Standard also used by other data providers, required  for Report Server 
                            _sortOrders.Add(String.Format("{0} [+]| BY {1}|{0}", c.ColumnName, _columnName));
                            _sortOrders.Add(String.Format("{0} [-]| BY {1} DESC|{0}", c.ColumnName, _columnName));
                        }
                    }

                    // Since LL26 - Support for FK Sort. We need to register all columns of all parent tables here.
                    // See also lldbcommandsetdataprovider. Currently there is no other way!
                    // Loop through all parent relations of the table. There may be more that one for the same parent table
                    // like BillingAddress and ShippingAdress for db services.
                    ReadOnlyCollection<ITableRelation> relations = Provider.Relations;
                    var groupedMatches = relations.Select(r => r)
                                .Where(r => r.ChildTableName == TableName)
                                .GroupBy(r => r.ParentTableName);
                    foreach (var group in groupedMatches)
                    {
                        foreach (OpenEdgeTableRelation relation in group)
                        {
                            OpenEdgeTableRow ParentSchema = Provider.GetMasterTable(relation.ParentTableName).SchemaRow as OpenEdgeTableRow;
                            OpenEdgeTableRow ChildSchema  = SchemaRow as OpenEdgeTableRow;

                            OpenEdgeTable parentTable = relation.ParentTable as OpenEdgeTable;

                            // build the relation fields for OpenEdge
                            string[] parentcolumns = relation.ParentColumnName.Split('\t');
                            string[] childcolumns  = relation.ChildColumnName.Split('\t');
                            string OERelationFields = "";

                            bool isStandardRelation = (relation.ParentColumnName == relation.ChildColumnName);

                            // We need the relation fields in Parent <-->> Child order for the OE DataSource Query in OpenEdgeService                            // 
                            for (int i = 0; i < parentcolumns.Length; i++)
                            {
                                OpenEdgeTableColumn ChildColumn  = ChildSchema.GetColumn(childcolumns[i]) as OpenEdgeTableColumn;
                                OpenEdgeTableColumn ParentColumn = ParentSchema.GetColumn(parentcolumns[i]) as OpenEdgeTableColumn;
                                if (i > 0) OERelationFields += ",";
                                OERelationFields += String.Format("{0},{1}", ParentColumn.OEColumnName, ChildColumn.OEColumnName);
                            }

                            string displayName = "";
                            string oesortident = "";
                            string tableColumn = "";

                            foreach (OpenEdgeTableColumn column in ParentSchema.Columns)
                            {
                                if (!column.Sortable) 
                                    continue;
                                // No support for calculated columns in FK (yet?!)
                                if (column.OECalculatedColumn == true) 
                                    continue;

                                if (isStandardRelation == true)
                                {
                                    displayName = String.Format("{0}.{1})", relation.ParentTableName, column.ColumnName);
                                }
                                else
                                {
                                    displayName = String.Format("{0}.{1} ({2})", relation.ParentTableName, column.ColumnName, relation.RelationName);
                                }

                                oesortident = String.Format("@{0}:{1}:{2}", parentTable.OETableName, OERelationFields, column.OEColumnName);
                                tableColumn = String.Format("{0}.{1}", relation.ParentTableName, column.ColumnName);

                                // The third value is used for the Used Identifiers. In case of FK Sort we don't need this, 
                                // since the service doesn't return values for FK Sort Columns together with the main table.
                                _sortOrders.Add(String.Format("{0} [+]| BY {1}|{2}", displayName, oesortident, tableColumn));
                                _sortOrders.Add(String.Format("{0} [-]| BY {1} DESC|{2}", displayName, oesortident, tableColumn ));
                            }
                        }
                    }
                }
                return _sortOrders.AsReadOnly();
            }
        }

        public bool SupportsAdvancedSorting
        {
            get
            {
                return true;
            }
        }

        public bool SupportsCount
        {
            get
            {
                return true;
            }
        }

        public bool SupportsFiltering
        {
            get
            {
                return true;
            }
        }

        public bool SupportsSorting
        {
            get
            {
                return true;
            }
        }

        public string TableName { get; private set; }

        public void ApplyFilter(string filter)
        {
            CurrentFilter = filter;
        }

        public void ApplySort(string sortDescription)
        {
            CurrentSort = sortDescription;
        }
        #endregion

        #region IAdvancedFiltering members 
        void IAdvancedFiltering.ApplyAdvancedFilter(string filter, object[] parameters)
        {
            CurrentAdvancedFilter = filter;
        }
        object IAdvancedFiltering.TranslateFilterSyntax(LlExpressionPart part, ref object name, int argumentCount, object[] arguments)
        {
            return OpenEdgeFilterSyntax(part, ref name, argumentCount, arguments);            
        }

        int InstanceId { get; set; }

        private object OpenEdgeFilterSyntax (LlExpressionPart part, ref object name, int argumentCount, object[] arguments)
        {
            char[] trimChars = { '\'', '"' }; // single quote, double quote
            // Delimiter for multivalue filter values passed to OpenEdge
            string valueSep = "|";

            // TODO: Handle numeric and date, datetime filter, special string filter ....
            string filtername;
            string valueList;
            switch (part)
            {
                case LlExpressionPart.Unknown:
                    {
                        return null;
                    }
                case LlExpressionPart.Boolean:
                    {
                        if (arguments[0] != null)
                        {
                            return ((bool)arguments[0] ? "TRUE" : "FALSE");
                        }
                        else
                        {
                            return "NULL";
                        }
                    }
                case LlExpressionPart.Text:
                    {
                        if (arguments[0] != null)
                        {
                            return String.Format("'{0}'", arguments[0]);
                        }
                        else
                        {
                            return "NULL";
                        }
                    }
                case LlExpressionPart.Number:
                    {
                        if (arguments[0] != null)
                        {
                            //return String.Format("'{0}'", (arguments[0] as IConvertible).ToString(CultureInfo.InvariantCulture));
                            return String.Format("'{0}'", Provider.OEStringValue(arguments[0]));
                        }
                        else
                        {
                            return "NULL";
                        }
                    }
                case LlExpressionPart.Date:
                    {
                        if (arguments[0] != null)
                        {
                            //return String.Format("'{0}'", arguments[0]);
                            return String.Format("'{0}'", Provider.OEStringValue(arguments[0]));
                        }
                        else
                        {
                            return "NULL";
                        }
                    }
                case LlExpressionPart.UnaryOperatorSign:
                    {
                        return (String.Format("(-({0}))", arguments[0]));
                    }
                case LlExpressionPart.UnaryOperatorNegation:
                    {
                        return (String.Format("(NOT ({0}))", arguments[0]));
                    }
                case LlExpressionPart.BinaryOperatorAdd:
                    {
                        return (String.Format("({0} + {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.BinaryOperatorAppend:
                    {
                        // is this a string append?
                        //return (String.Format("({0} + {1})", arguments[0], arguments[1]));
                        return null;
                    }
                case LlExpressionPart.BinaryOperatorAddDateTime:
                    {
                        // OK ?
                        //return (String.Format("({0} + {1})", arguments[0], arguments[1]));
                        return null;
                    }
                case LlExpressionPart.BinaryOperatorSubtract:
                    {
                        return (String.Format("({0} - {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.BinaryOperatorMultiply:
                    {
                        return (String.Format("({0} * {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.BinaryOperatorDivide:
                    {
                        return (String.Format("({0} / {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.BinaryOperatorModulo:
                    {
                        return null;
                    }
                case LlExpressionPart.RelationXor:
                    {
                        return (String.Format("(({0} AND NOT {1}) OR (NOT {0} AND {1}))", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationOr:
                    {
                        return (String.Format("({0} OR {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationAnd:
                    {
                        return (String.Format("({0} AND {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationEqual:
                    {
                        if (!name.ToString().Equals("@ARG:MULTIVALUE", StringComparison.Ordinal))
                        {
                            return (String.Format("({0} = {1})", arguments[0], arguments[1]));
                        }
                        else
                        {
                            if ((string)arguments[1] != String.Empty)
                            {
                                // Store multi value filters in the table master
                                // Picked up an cleared by OpenQuery().
                                OpenEdgeTable TableMaster = Provider.GetMasterTable(this.TableName) as OpenEdgeTable;
                                filtername = string.Format("[OEMultiValueFilter{0}]", TableMaster.CurrentMultiValueFilters.Count);
                                valueList = arguments[1].ToString().Replace(",'", valueSep).Replace("'", "");
                                OpenEdgeMultiValueFilter m = new OpenEdgeMultiValueFilter(TableMaster, filtername, arguments[0].ToString(), true, valueSep, valueList);
                                TableMaster.CurrentMultiValueFilters.Add(m);
                                return string.Format("({0})", filtername);
                            }
                            else
                                return "FALSE";
                            /*
                            if ((string)arguments[1] != String.Empty)
                                return (String.Format("({0} IN ({1}))", arguments[0], NullSafeSqlValue(arguments[1])));
                            else
                                return "(1=0)";
                            */
                        }
                    }
                case LlExpressionPart.RelationNotEqual:
                    {
                        if (!name.ToString().Equals("@ARG:MULTIVALUE", StringComparison.Ordinal))
                        {
                            return (String.Format("({0} <> {1})", arguments[0], arguments[1]));
                        }
                        else
                        {
                            if ((string)arguments[1] != String.Empty)
                            {
                                // Store multi value filters in the table master 
                                // Picked up an cleared by OpenQuery().
                                OpenEdgeTable TableMaster = Provider.GetMasterTable(this.TableName) as OpenEdgeTable;
                                filtername = string.Format("[OEMultiValueFilter{0}]", TableMaster.CurrentMultiValueFilters.Count);
                                valueList = arguments[1].ToString().Replace(",'", valueSep).Replace("'", "");
                                OpenEdgeMultiValueFilter m = new OpenEdgeMultiValueFilter(TableMaster, filtername, arguments[0].ToString(), false, valueSep, valueList);
                                TableMaster.CurrentMultiValueFilters.Add(m);
                                return string.Format("({0})", filtername);
                            }
                            else
                                return "(TRUE)";
                            /*
                            if ((string)arguments[1] != String.Empty)
                                return (String.Format("(NOT ({0} IN ({1})))", arguments[0], NullSafeSqlValue(arguments[1])));
                            else
                                return "(1=1)";
                            */
                        }
                    }
                case LlExpressionPart.RelationGreaterThan:
                    {
                        return (String.Format("({0} > {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationGreaterThanOrEqual:
                    {
                        return (String.Format("({0} >= {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationLessThan:
                    {
                        return (String.Format("({0} < {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.RelationLessThanOrEqual:
                    {
                        return (String.Format("({0} <= {1})", arguments[0], arguments[1]));
                    }
                case LlExpressionPart.Function:
                    {
                        string value;
                        switch (name.ToString().ToUpper())
                        {
                            case "":
                                return (String.Format("({0})", arguments[0]));
                            case "ISNULL":
                                return (String.Format("({0} = ?)", arguments[0]));
                            case "STARTSWITH":
                                value = arguments[1].ToString().Trim(trimChars);
                                return (String.Format("({0} BEGINS '{1}')", arguments[0], value));
                            case "ENDSWITH":
                                value = arguments[1].ToString().Trim(trimChars);
                                return (String.Format("({0} MATCHES '*{1}')", arguments[0], value));
                            case "CONTAINS":
                                // TODO: Wordindex - that's hard ... almost impossible ... later :-)
                                value = arguments[1].ToString().Trim(trimChars);
                                return (String.Format("({0} MATCHES '*{1}*')", arguments[0], value));
                            case "YEAR":
                                return (String.Format("(YEAR({0}))", arguments[0]));
                            case "MONTH":
                                return (String.Format("(MONTH({0}))", arguments[0]));
                            case "DAY":
                                return (String.Format("(DAY({0}))", arguments[0]));
                            case "UPPER$":
                                return (String.Format("(CAPS({0}))", arguments[0]));
                            case "LOWER$":
                                return (String.Format("(LC({0}))", arguments[0]));
                            case "MID$":
                                /*
                                if (argumentCount == 2)
                                    return (String.Format("(SUBSTRING({0},{1}))", arguments[0], arguments[1].ToString() + "+1"));
                                else
                                    return (String.Format("(SUBSTRING({0},{1},{2}))", arguments[0], arguments[1].ToString() + "+1", arguments[2]));
                                */
                                return null;
                            case "LEFT$":
                                //return (String.Format("(SUBSTRING({0},1,{1}))", arguments[0], arguments[1]));
                                return null;
                            case "RIGHT$":
                                //return (String.Format("(RIGHT({0},{1}))", arguments[0], arguments[1]));
                                return null;
                            case "LEN":
                                //return (String.Format("(LEN({0}))", arguments[0]));
                                return null;
                            case "EMPTY":
                                /*
                                if (argumentCount == 1)
                                    return (String.Format("(LEN({0}) = 0)", arguments[0]));
                                else
                                    if ((bool)arguments[1])
                                    return (String.Format("(LEN(LTRIM(RTRIM({0}))) = 0)", arguments[0]));
                                else
                                    return (String.Format("(LEN({0}) = 0)", arguments[0]));
                                */
                                return null;
                            case "ROUND":
                                //return (String.Format("(ROUND({0},{1}))", arguments[0], argumentCount == 2 ? arguments[1] : "0"));
                                return null;
                            case "DATEINRANGE":
                                //return (String.Format("(({0} >= {1}) AND ({0} <= {2}))", arguments[0], arguments[1], arguments[2]));
                                return null;
                            case "NUMINRANGE":
                                //return (String.Format("(({0} >= {1}) AND ({0} <= {2}))", arguments[0], arguments[1], arguments[2]));
                                return null;
                            case "ATRIM$":
                                //return (String.Format("(LTRIM(RTRIM({0})))", arguments[0]));
                                return null;
                            case "LTRIM$":
                                //return (String.Format("(LTRIM({0}))", arguments[0]));
                                return null;
                            case "RTRIM$":
                                //return (String.Format("(RTRIM({0}))", arguments[0]));
                                return null;

                            // http://technet.microsoft.com/de-de/library/ms186819.aspx

                            case "ADDDAYS":
                                /*if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEADD(d,{1},{0}))", arguments[0], arguments[1]));*/
                                return null;
                            case "ADDMONTHS":
                                /*if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEADD(m,{1},{0}))", arguments[0], arguments[1]));*/
                                return null;
                            case "ADDYEARS":
                                /*if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEADD(yy,{1},{0}))", arguments[0], arguments[1]));
                                */
                                return null;
                            case "ADDHOURS":
                                /*if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEADD(hh,{1},{0}))", arguments[0], arguments[1]));
                                */
                                return null;
                            case "ADDMINUTES":
                                /*
                                if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEADD(mi,{1},{0}))", arguments[0], arguments[1]));
                                */
                                return null;
                            case "ADDSECONDS":
                                /*if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEADD(s,{1},{0}))", arguments[0], arguments[1]));
                                */
                                return null;
                            case "ADDWEEKS":
                                /*
                                if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEADD(wk,{1},{0}))", arguments[0], arguments[1]));
                                */
                                return null;

                            // http://msdn.microsoft.com/de-de/library/ms189794.aspx

                            case "DATEDIFF":
                                /*
                                if (_dbCommandType != DbCommandType.SqlCommand)
                                    return null;
                                return (String.Format("(DATEDIFF(d,{0},{1}))", arguments[0], arguments[1]));
                                */
                                return null;

                        }
                        return null;
                    }

                case LlExpressionPart.Field:
                    {
                        string identifier = arguments[0].ToString();
                        Provider.DebugOutput(string.Format("field identifier: {0}", identifier));
                        // We just have to check if we got <Table>.<Column> and extract column.
                        // If it's something else with ":" or "@" we will not be able to find the columns later und return null;
                        // So this should work ...
                        string[] parts = identifier.Split('.');
                        string columnName = parts[parts.Length - 1];
                        OpenEdgeTableRow Schema = this.SchemaRow as OpenEdgeTableRow;
                        if (Schema.GetColumn(columnName) is OpenEdgeTableColumn Column)
                        {
                            string OEColumnName;
                            if (Column.OEColumnIndex == 0)
                                OEColumnName = Column.OEColumnName;
                            else
                                OEColumnName = String.Format("{0}[{1}]", Column.OEColumnName, Column.OEColumnIndex);
                            return OEColumnName;
                        }
                        return null;
                    }
                default:
                    return null;
            }

        }


        #endregion

        #region OpenEdgeTable properties
        public OpenEdgeDataProvider Provider { get; protected set; }
        public string OETableName { get; protected set; }
        public string OEDbTableName { get; protected set; }
        public bool   OECalculatedTable { get; protected set; }
        public bool   OECachedTable { get; protected set; }

        //public string OEDatabaseName { get; protected set; }
        public string BaseQueryWhere { get; internal set; }
        public string InitialSortBy { get; internal set; }

        #endregion

        internal void SetParentRelationQuery(RelationQuery query)
        {
            ParentRelationQuery = query;
        }

        // Constructor 
        public OpenEdgeTable(OpenEdgeDataProvider dataprovider, string tablename, string oetablename, string oedbtablename, bool oecalculatedtable, bool oecachedtable)
        {
            Provider = dataprovider;
            TableName = tablename;
            OETableName = oetablename;
            OEDbTableName = oedbtablename;
            OECalculatedTable = oecalculatedtable;
            OECachedTable = oecachedtable;
            _tablerows = new List<OpenEdgeTableRow>();
            _enumerator = new OpenEdgeTableRowEnumerator(this);
            CurrentMultiValueFilters = new List<OpenEdgeMultiValueFilter>();
            BaseQueryWhere = "";
            InitialSortBy = "";
        }

        // Since LL26 - Create a table clone. 
        // Required for multiple instances of the same table i.e. for recursive relations.
        public OpenEdgeTable Clone()
        {
            OpenEdgeTable clone = new OpenEdgeTable(Provider, TableName, OETableName, OEDbTableName, OECalculatedTable, OECachedTable);
            clone.BaseQueryWhere = BaseQueryWhere;
            clone.InitialSortBy = InitialSortBy;
            clone.InstanceId = Provider.NextTableInstanceId();
            // Use the schema row from the master. In case it's required.
            clone.SchemaRow = SchemaRow;
            // Copy the identifiers. These are only set once per report by LL 
            if (_thisTableJoinedIdentifiers != null) 
                clone.SetJoinedIdentifiers(_thisTableJoinedIdentifiers);
            if (_thisTableUsedIdentifiers != null)
                clone.SetUsedIdentifiers(_thisTableUsedIdentifiers);
            return clone;
        }

        public void SetUsedIdentifiers(ReadOnlyCollection<string> identifiers)
        {
            List<string> OEColumns = new List<string>();
            OpenEdgeTableColumn OpenEdgeColumn;
            ITableColumn Column;
            OpenEdgeTableRow Schema;
            _thisTableUsedIdentifiers = identifiers;

            foreach (string name in identifiers)
            {
                Schema = (OpenEdgeTableRow)SchemaRow;
                Column = Schema.GetColumn(name);
                // 20191010 - GetColumn returns null if an identifier could not be matched with a column.
                // If a column has "_" in it's name LL sends two identifiers, i.e. "Cust_Num" and "Cust-Num".
                // "Cust-Num" failed with a null pointer exception.
                if (Column != null)
                {
                    OpenEdgeColumn = (OpenEdgeTableColumn)Column;
                    OEColumns.Add(OpenEdgeColumn.OEColumnName);
                }
            }
            UsedOETableColumns = String.Join(",", OEColumns.Distinct());
            if (UsedOETableColumns.Length > 0)
                Provider.DebugOutput(String.Format("Table {0} - Used OETableColumns: {1}", TableName, UsedOETableColumns));
        }

        public void SetJoinedIdentifiers(ReadOnlyCollection<string> identifiers)
        {
            _thisTableJoinedIdentifiers = identifiers;
            if (identifiers.Count > 0)
            {
                Provider.DebugOutput(String.Format("Table {0} - JoinedIdentifiers: {1}", TableName, String.Join(",", identifiers)));
            }
        }

        // Methods the get data from OpenEdge and to navigate in the query resultset.
        public void OpenQuery()
        {
            ResetQuery();
            OpenEdgeTable TableMaster = Provider.GetMasterTable(this.TableName) as OpenEdgeTable;

            // Lets get the data ...
            if (UsedOETableColumns.Length > 0)
            {
                string filter = "";
                if (BaseQueryWhere != "")
                {
                    filter = BaseQueryWhere;
                }

                if (CurrentFilter != "")
                {
                    if (filter != "")
                        filter += " AND ";
                    filter += CurrentFilter;
                }
                if (CurrentAdvancedFilter != "")
                {
                    if (filter != "")
                        filter += " AND ";
                    filter += CurrentAdvancedFilter;
                }

                Provider.DebugOutput(string.Format("Table {0}: Filter {1}", TableName, filter));

                OpenEdgeQuery theQuery = new OpenEdgeQuery(this, _thisTableJoinedIdentifiers);

                // TODO: Move the request execution into the query
                // 20170816 - use initial sort if there is no sort defined in the layout 
                if (CurrentSort == "" && InitialSortBy != "" )
                        CurrentSort = InitialSortBy;

                // Since LL26: Filters for multiple select report parameters are collected by OpenEdgeFilterSyntax()
                // and are stored in master table (not in 'this' table). We pick them up here and clear them when the request has been build. 
                // (LL creates another clone just for translateFilterSyntax())
                OpenEdgeDataRequest _request = theQuery.BuildRequest(ParentRelationQuery, filter, TableMaster.CurrentMultiValueFilters ,CurrentSort.Replace("\t", ""));
                TableMaster.CurrentMultiValueFilters.Clear();

                Provider.DebugOutput(string.Format("Table {0}: Request defined.", TableName));
                string jsonRequest;
                try
                {
                    jsonRequest = _request.GetJson();
                }
                catch (JsonException e)
                {
                    Provider.DebugOutput(string.Format("Table {0}: JsonError: {1}", TableName, e.Message));
                    return;
                }
                if (jsonRequest == null)
                    return;
                Provider.DebugOutput(string.Format("Table {0}: Json created.", TableName));

                OELongchar jsonParameter = new OELongchar();
                Provider.ServiceParameter.AssignDefaultServiceParameter();
                jsonParameter.Data = Provider.ServiceParameter.GetJson();

                OELongchar jsonDataRequest = new OELongchar
                {
                    Data = jsonRequest
                };
                Provider.DebugOutput(String.Format("Table {0} - Request for Query: {1}", TableName, jsonRequest));
                _ = Provider.ServiceAdapter.GetData(Provider.ServiceName, jsonParameter, jsonDataRequest, out OELongchar jsonDataResponse);

                Provider.DebugOutput(String.Format("Table {0} - Response: {1} bytes", TableName, jsonDataResponse.Data.Length));
                _ = new OpenEdgeResponseReader(Provider, _request, jsonDataResponse.Data);
            }

            // Reset everything after the query has been executed.
            CurrentAdvancedFilter = "";
            CurrentFilter = "";
            CurrentSort = "";
            ParentRelationQuery = null;
            //TableMaster.CurrentMultiValueFilters.Clear();
        }
        public bool GetNextTableRow()
        {
            position++;
            return position < _tablerows.Count;
        }

        public ITableRow GetCurrentTableRow()
        {
            return _tablerows[position];
        }

        public void ResetQuery()
        {
            _tablerows.Clear();
            position = -1;
        }

        public void AddOpenEdgeTableRow(OpenEdgeTableRow row)
        {
            _tablerows.Add(row);
        }

        public void AddOpenEdgeForeignTableRow(ITableRelation relation, OpenEdgeTableRow row)
        {
            OpenEdgeParentRowCache cache = null;

            foreach (OpenEdgeParentRowCache c in _openEdgeParentRowCaches)
            {
                if (c.Relation == relation)
                {
                    cache = c;
                    break;
                }
                    
            }
            if (cache == null)
            {
                cache = new OpenEdgeParentRowCache(this, relation);
                _openEdgeParentRowCaches.Add(cache);
            }
            cache.AddForeignKey(row);
        }

        public ITableRow getOpenEdgeForeignTableRow (OpenEdgeTableRow row, OpenEdgeTableRelation relation)
        {
            foreach (OpenEdgeParentRowCache c in _openEdgeParentRowCaches)
            {
                if (c.Relation == relation)
                {
                    return c.GetForeignKey(row);
                }
            }
            return null;
        }

        public object ExecuteNativeAggregateFunction(ExecuteNativeAggregateFunctionArguments args)
        {
            if (Provider.DesignMode)
                return null;
            OpenEdgeNativeAggregateFunction f = new OpenEdgeNativeAggregateFunction(this,args);
            return f.Execute();
        }

        public bool CheckNativeAggregateFunctionSyntax(CheckNativeAggregateFunctionSyntaxArguments args)
        {
            //throw new NotImplementedException();
            // TODO: Check at least the expression 
            return true;
        }

        public bool SupportsNativeAggregateFunction(NativeAggregateFunction function)
        {
            bool supported = false;
            switch (function)
            {
                case NativeAggregateFunction.Avg:
                    supported = true;
                    break;
                case NativeAggregateFunction.Count:
                    supported = true;
                    break;
                case NativeAggregateFunction.Max:
                    supported = true;
                    break;
                case NativeAggregateFunction.Min:
                    supported = true;
                    break;
                case NativeAggregateFunction.StdDevPop:
                    break;
                case NativeAggregateFunction.StdDevSamp:
                    break;
                case NativeAggregateFunction.Sum:
                    supported = true;
                    break;
                case NativeAggregateFunction.VarPop:
                    break;
                case NativeAggregateFunction.VarSamp:
                    break;
            }
            return supported;
        }
    }

    // The TableRowEnumerator. 
    internal class OpenEdgeTableRowEnumerator : IEnumerator<ITableRow>
    {
        private OpenEdgeTable _table = null;

        // constructor 
        public OpenEdgeTableRowEnumerator(OpenEdgeTable Table)
        {
            _table = Table;
        }

        // get the current table row from OpenEdge
        public ITableRow Current
        {
            get
            {
                return _table.GetCurrentTableRow();
            }
        }

        // Anything to do here ?
        public void Dispose()
        {
        }

        // move the OpenEdge query to the next record. 
        public bool MoveNext()
        {
            return _table.GetNextTableRow();
        }

        public void Reset()
        {
            _table.ResetQuery();
        }
        #region IEnumerator Members

        object IEnumerator.Current
        {
            get { return Current; }
        }

        #endregion
    }

    internal class OpenEdgeMultiValueFilter
    {
        public ITable Table   { get; private set; }
        public ITableColumn Column { get; private set; }
        public string OETableName { get; private set; }
        public string OEColumnName { get; private set;}
        public string OEDataType { get; private set; }
        public string FilterName   { get; private set; }
        public string ValueDelimiter { get; private set; }
        public string Values { get; private set; }
        public bool InOperator { get; private set; }
        public OpenEdgeMultiValueFilter (ITable table, string filtername, string columnname , bool inoperator , string delimiter, string values )
        {
            Table      = table;
            OpenEdgeTableRow    Schema   = Table.SchemaRow as OpenEdgeTableRow;
            OpenEdgeTableColumn OEColumn = Schema.GetColumn(columnname) as OpenEdgeTableColumn;
            Column = OEColumn;
            OpenEdgeTable OETable = table as OpenEdgeTable;
            OETableName = OETable.OETableName;
            OEColumnName = OEColumn.OEColumnName;
            OEDataType = OEColumn.OEDataType;
            FilterName    = filtername;
            Values = values;
            InOperator = inoperator;
            ValueDelimiter = delimiter;
        }
    }

    // An OpenEdge Table Relation 
    public class OpenEdgeTableRelation : ITableRelation
    {
        private OpenEdgeDataProvider _provider;
        public string ChildColumnName { get; private set; }
        public string ChildTableName { get; private set; }
        public string ParentColumnName { get; private set; }
        public string ParentTableName { get; private set; }
        public string RelationName { get; private set; }
        public string ForeignKeyIdentifier { get; private set; }
        public string RelationIdentifier { get; private set; }
        public OpenEdgeDataProvider DataProvider { get; private set; }
        public ITable ParentTable { get; set; }
        public ITable ChildTable  { get; set; }

        private Dictionary<string, ITableRow> FKCache = new Dictionary<string, ITableRow>();
        private string valueSep;

        // Constructor 
        public OpenEdgeTableRelation(OpenEdgeDataProvider dataprovider, string parenttablename, string childtablename, string parentcolumnname, string childcolumnname, string relationname)
        {
            _provider        = dataprovider;
            ParentTableName  = parenttablename;
            ChildTableName   = childtablename;
            ParentColumnName = parentcolumnname;
            ChildColumnName  = childcolumnname; ;
            RelationName     = relationname;
            valueSep         = _provider.ValueDelimiter.ToString();

            ParentTable = _provider.GetMasterTable(ParentTableName);
            ChildTable  = _provider.GetMasterTable(ChildTableName);

            // Identifier Parent -> Child
            RelationIdentifier = String.Format("{0}.{1}@{2}.{3}", ParentTableName, ParentColumnName.Replace("\t", "_"), ChildTableName, ChildColumnName.Replace("\t", "_"));
            // Identifier Child - Parent as used in JoinedIdentifiers */
            ForeignKeyIdentifier = String.Format("{0}.{1}@{2}.{3}", ChildTableName, ChildColumnName.Replace("\t", "_"), ParentTableName, ParentColumnName.Replace("\t", "_"));
        }
    }

    internal class RelationQuery
    {
        public string OEParentTableName   { get; set; }
        public string OERelationFields    { get; set; }
        public string OEParentWhere       { get; set; }
        public string OEChildWhere        { get; set; }
    }

    // An OpenEdge Table Row
    public class OpenEdgeTableRow : ITableRow
    {
        private OpenEdgeDataProvider _provider = null;
        //private List<ITableColumn> _columns = null;
        private Dictionary<string, ITableColumn> _dictionary;
        private ITable _table;
        public ReadOnlyCollection<ITableColumn> Columns
        {
            get
            {
                return _dictionary.Values.ToList<ITableColumn>().AsReadOnly();
            }
        }
        public bool SupportsGetParentRow { get { return true; } }
        public string TableName { get { return _table.TableName; } }
        public ITable GetChildTable(ITableRelation relation)
        {
            // Create a WhereClause for the child table.
            // ChildTable.Key1 = <Column1.Value> [AND ChildTable.Key2 = <Column2.Value> ...]
            OpenEdgeTableRelation _relation = relation is OpenEdgeTableRelation
                ? (OpenEdgeTableRelation)relation
                : this._provider.GetRelation(relation.RelationName) as OpenEdgeTableRelation;

            // 20181128 
            // The normal case is that the OpenEdgeRelation will be found ... 

            if (_relation == null)
                return null;

            // Since LL26 - Create Clone for the ChildTable.
            OpenEdgeTable OpenEdgeChildTable = (_relation.ChildTable as OpenEdgeTable).Clone();

            OpenEdgeTable OpenEdgeParentTable = _relation.ParentTable as OpenEdgeTable;
            string[] _ChildColummNames = _relation.ChildColumnName.Split('\t');
            string[] _ParentColummNames = _relation.ParentColumnName.Split('\t');
            string ChildWhereClause = "";
            string ParentWhereClause = "";
            OpenEdgeTableRow ParentSchema = OpenEdgeParentTable.SchemaRow as OpenEdgeTableRow;
            OpenEdgeTableRow ChildSchema  = OpenEdgeChildTable.SchemaRow  as OpenEdgeTableRow;

            string Operator = "";
            for (int iPair = 0; iPair < _ChildColummNames.Length; iPair++)
            {
                OpenEdgeTableColumn ParentColumn = GetColumn(_ParentColummNames[iPair]) as OpenEdgeTableColumn;
                OpenEdgeTableColumn ChildSchemaColumn = ChildSchema.GetColumn(_ChildColummNames[iPair]) as OpenEdgeTableColumn;
                OpenEdgeTableColumn ParentSchemaColumn = ParentSchema.GetColumn(_ParentColummNames[iPair]) as OpenEdgeTableColumn;
                // We don't support Relations on array fields. So we don't have to care about it 
                // in a where clause based on a relation.
                // TODO: We may have to format the value to something that OpenEdge understands. This may be a method in the OpenEdgeColumn Class.
                ChildWhereClause  = ChildWhereClause  + Operator + String.Format("{0} = '{1}'", ChildSchemaColumn.OEColumnName,_provider.OEStringValue(ParentColumn.Content));
                ParentWhereClause = ParentWhereClause + Operator + String.Format("{0} = '{1}'", ParentSchemaColumn.OEColumnName, _provider.OEStringValue(ParentColumn.Content));
                Operator = " AND ";
            }
            OpenEdgeChildTable.Provider.DebugOutput(String.Format("Table {0}:GetChildTable {1}: ChildQuery: {2}", TableName, OpenEdgeChildTable.TableName, ChildWhereClause));

            RelationQuery ParentRelationQuery = new RelationQuery
            {
                OEParentTableName = OpenEdgeParentTable.OETableName,
                OEParentWhere = ParentWhereClause,
                OEChildWhere = ChildWhereClause
            };
            OpenEdgeChildTable.SetParentRelationQuery(ParentRelationQuery);
            return OpenEdgeChildTable as ITable;
        }
        public ITableRow GetParentRow(ITableRelation relation)
        {
            if (relation != null)
            {
                OpenEdgeTableRelation _relation = !(relation is OpenEdgeTableRelation)
                    ? this._provider.GetRelation(relation.RelationName) as OpenEdgeTableRelation
                    : (OpenEdgeTableRelation)relation;

                // 20181128 
                // The normal case is that the OpenEdgeRelation will be found ... 

                if (_relation == null)
                    return null;

                if (_provider.DesignMode)
                {
                    return _relation.ParentTable.SchemaRow;
                }

                OpenEdgeTable table = _table as OpenEdgeTable;
                return table.getOpenEdgeForeignTableRow(this, _relation);
            }

            return null;
        }
        // Constructor 
        public OpenEdgeTableRow(OpenEdgeTable table)
        {
            OpenEdgeTable t = table as OpenEdgeTable;
            _provider = t.Provider;
            _table = table;
            //_columns = new List<ITableColumn>();
            _dictionary = new Dictionary<string, ITableColumn>();
        }

        public void AddColumn(OpenEdgeTableColumn column)
        {
            //_columns.Add(column);
            _dictionary.Add(column.ColumnName.ToLower(), column);
        }

        public ITableColumn GetColumn(string columnName)
        {            
            return _dictionary.TryGetValue(columnName.ToLower(), out ITableColumn column) ? column : null;
        }
    }

    public class OpenEdgeTableColumn : ITableColumn
    {
        private ITable _table;
        public string ColumnName { get; private set; }
        public object Content { get; private set; }
        public Type DataType { get; private set; }
        public LlFieldType FieldType { get; private set;}
        public string OEColumnName { get; private set; }
        public string OEDataType { get; private set; }
        public string OEMimeType { get; private set; }
        public int OEColumnIndex { get; private set; }
        public bool OECalculatedColumn { get; private set; }
        public bool OECaseSensitive { get; private set; }

        public string FileExtension { get; private set; }

        public bool Sortable { get; private set; }

        public OpenEdgeTableColumn SchemaColumn { get; private set; }

        private string _type;

        // Contructor for a data column - created during OpenEdgeDataResponse
        // Data column in the proper system data type.
        public OpenEdgeTableColumn(ITable table, string columnname, object jsonValue)
        {
            OpenEdgeTableRow    schema       = table.SchemaRow as OpenEdgeTableRow;
            OpenEdgeTableColumn schemaColumn = schema.GetColumn(columnname) as OpenEdgeTableColumn;

            _table = table;
            SchemaColumn = schemaColumn;
            DataType   = schemaColumn.DataType;
            OECaseSensitive = schemaColumn.OECaseSensitive;
            ColumnName = columnname;
            FieldType  = LlFieldType.Unknown;

            Content = null;

            // Json has only 3 DataTypes: Number, String, Boolean true/false.
            // The jsonValue has one of these 3 types.
            if (jsonValue == null)
                return;

            // Images. It works if we just return the byte[] array. 
            // But combit recommends to do it this way. Writing the image to the disk and return the path.
            // (For better quality)
            if (schemaColumn.OEDataType.ToLower() == "blob")
            {
                byte[] imageBytes = Convert.FromBase64String(jsonValue.ToString());
                OpenEdgeTable t = _table as OpenEdgeTable;
                string tempFile = t.Provider.GetTempFileName(schemaColumn.FileExtension);
                MemoryStream ms = new MemoryStream(imageBytes);
                Image image = Image.FromStream(ms, true, true);
                image.Save(tempFile);
                Content = tempFile;
            }
            // We may have to do something for date, datetime and datetime-tz. 
            // But just changing the type seems to work also.
            else
                Content = Convert.ChangeType(jsonValue, DataType);
        }

        // Constructor - for a schema column. A schema column has no data but all the schema information for OE.  
        public OpenEdgeTableColumn(ITable table, string columnname, string oecolumnname, int oecolumnindex, string oedatatype, string oemimetype, string oecolumnvalue,bool oecalculatedcolumn,bool oecasesensitive)
        {
            _table = table;
            FieldType = LlFieldType.Unknown;
            ColumnName = columnname;
            OEColumnName = oecolumnname;
            OEColumnIndex = oecolumnindex;
            OEDataType = oedatatype.ToLower();
            OECalculatedColumn = oecalculatedcolumn;
            OECaseSensitive = oecasesensitive;
            if (OEColumnIndex == 0) Sortable = true;
            else Sortable = false;

            // normal type mapping from OE to system
            switch (OEDataType)
            {
                case "integer":
                    _type = "System.Int32";
                    break;
                case "int64":
                    _type = "System.Int64";
                    break;
                case "decimal":
                    _type = "System.Decimal";
                    break;
                case "logical":
                    _type = "System.Boolean";
                    break;
                case "date":
                    _type = "System.DateTime";
                    break;
                case "datetime":
                    _type = "System.DateTime";
                    break;
                case "datetime-tz":
                    _type = "System.DateTime";
                    break;
                case "character":
                    _type = "System.String";
                    break;
                case "clob":
                    _type = "System.String";
                    Sortable = false;
                    break;
                case "blob":
                    _type = "System.Byte[]";
                    OEMimeType = oemimetype.ToLower();
                    OpenEdgeTable t = _table as OpenEdgeTable;
                    FileExtension = t.Provider.GetDefaultFileExtension(OEMimeType);
                    FieldType = LlFieldType.Drawing;
                    Sortable = false;
                    break;
                default:
                    _type = "System.String";
                    Sortable = false;
                    break;
            }

            DataType = Type.GetType(_type);
            Content = null;
            // TODO: Handle ? Value from OE. Depends on _type . Decimal, Date and numeric format - do we have to care about this?
            if (oecolumnvalue.Length > 0 && oecolumnvalue != "?")
            {
                Content = Convert.ChangeType(oecolumnvalue, DataType);
            }

            if (OEMimeType != "")
            {
                RegistryKey key;
                object value;
                key = Registry.ClassesRoot.OpenSubKey(@"MIME\Database\Content Type\" + OEMimeType, false);
                value = key?.GetValue("Extension", null);
                FileExtension = value != null ? value.ToString() : ".tmp";
            }
        }
    }

    internal class OpenEdgeParentRowCache
    {
        public OpenEdgeTable ChildTable { get; private set; }
        public ITableRelation Relation { get; private set; }

        private Dictionary<string, ITableRow> FKCache = new Dictionary<string, ITableRow>();

        private OpenEdgeDataProvider _provider;

        private string valueSep;

        private OpenEdgeTableRow SchemaRow;

        public OpenEdgeParentRowCache(ITable childTable, ITableRelation relation)
        {
            ChildTable = childTable as OpenEdgeTable;
            Relation = relation;
            _provider = (ChildTable as OpenEdgeTable).Provider;
            valueSep = _provider.ValueDelimiter.ToString();
            SchemaRow = _provider.GetMasterTable(childTable.TableName).SchemaRow as OpenEdgeTableRow;
        }

        public void AddForeignKey(ITableRow ParentRow)
        {
            string[] ColumnNames = Relation.ParentColumnName.Split('\t');
            List<string> values = new List<string>();
            for (int i = 0; i < ColumnNames.Length; i++)
            {
                values.Add(ColumnStringValue(ParentRow, ColumnNames[i]));
            }
            string key = string.Join(valueSep, values);
            if (!FKCache.ContainsKey(key))
            {
                //_provider.debugOutput(0, String.Format("added FK {0}({1}) into relation {2}", ParentRow.TableName, key, RelationName));
                FKCache.Add(key, ParentRow);
            }
        }

        public ITableRow GetForeignKey(ITableRow ChildRow)
        {
            string[] ColumnNames = Relation.ChildColumnName.Split('\t');
            List<string> values = new List<string>();
            for (int i = 0; i < ColumnNames.Length; i++)
            {
                values.Add(ColumnStringValue(ChildRow, ColumnNames[i]));
            }
            string key = string.Join(valueSep, values);

            if (FKCache.TryGetValue(key, out ITableRow ParentRow))
            {
                //_provider.debugOutput(0, String.Format("returned FK {0}({1}) from relation {2}", ParentRow.TableName, key, RelationName));
                return ParentRow;
            }
            else
            {
                // We have a problem if the key is not empty. Then a foreign key record was not returned by OpenEdge properly. */
                /* For now we don't care ...
                if (!String.IsNullOrWhiteSpace(key))
                    _provider.debugOutput(0, String.Format("FK {0}({1}) not found in relation {2}", ParentTableName, key, RelationName));
                */
            }
            return null;
        }

        private string ColumnStringValue(ITableRow row, string columnname)
        {
            OpenEdgeTableRow OpenEdgeRow = row as OpenEdgeTableRow;
            OpenEdgeTableColumn column = OpenEdgeRow.GetColumn(columnname) as OpenEdgeTableColumn;
            if (column != null && column.Content != null)
            {
                // Strings in the cache may be a problem. So we store everything in lowercase case except when the OEColumn is case sensitive.
                return column.OECaseSensitive ? _provider.OEStringValue(column.Content) : _provider.OEStringValue(column.Content).ToLower();
            }
            return "";
        }
    }

    internal class OpenEdgeView
    {
        public string ViewName      { get; private set; }
        public string ViewTables    { get; private set; }
        public string ViewRelations { get; private set; }

        public OpenEdgeView( string Name, string Tables, string Relations )
        {
            ViewName      = Name;
            ViewTables    = Tables;
            ViewRelations = Relations;
        }
    }

    // NativeAggregateFunction
    // To make it fast, everything is done internally and not done with the Query 
    internal class OpenEdgeNativeAggregateFunction
    {
        public string OETableName    { get; private set; }
        public string OEDbTableName  { get; private set; }
        public bool   OECalculatedTable { get; private set; }
        public string OETableWhere   { get; private set; }
        public string OETableColumns { get; private set; }
        public string OECalculatedColumns { get; private set; }
        public string OEFunctionName { get; private set; }
        public string OEExpression   { get; private set; }
        public bool   OEDistinct     { get; private set; }
        public string OEFunctionCallGuid { get; private set; }
        public string Parameter1         { get; private set; }
        public string Parameter2         { get; private set; }

        OpenEdgeDataProvider _provider = null;
        OpenEdgeTable _caller = null;
        OpenEdgeTable _table = null;

        public OpenEdgeNativeAggregateFunction (ITable Caller, ExecuteNativeAggregateFunctionArguments args)
        {
            _caller   = Caller as OpenEdgeTable;
            _provider = _caller.Provider;
            _table    = _provider.GetMasterTable(args.TableName) as OpenEdgeTable;

            OEFunctionCallGuid = Guid.NewGuid().ToString();
            OETableName        = _table.OETableName;
            OEDbTableName      = _table.OEDbTableName;
            OECalculatedTable  = _table.OECalculatedTable;
            OETableWhere       = args.Filter;     // Translated filter with OEColumns
            OEDistinct         = args.Distinct;   // Just needed for count?
            OEExpression       = args.Expression; // Translated Expression with OEColumns
            OEFunctionName     = OpenEdgeFunctionName(args.Function);
            Parameter1 = args.Parameter1;
            Parameter2 = args.Parameter2;

            // TODO: We may move this code to an evaluator class or into the table 
            _provider.DebugOutput(string.Format("NATIVE FUNCTION: {0}", args.Function));
            _provider.DebugOutput(string.Format("Parameter1: {0}", args.Parameter1));
            _provider.DebugOutput(string.Format("Parameter2: {0}", args.Parameter2));
            string s = args.Parameter1.Replace(args.TableName + "."," ");
            _provider.DebugOutput(string.Format("Parameter1 without Tablename: {0}", s));

            // This is a list of things to remove from the expression and to split the token 
            List<string> _OEColumns = new List<string>();
            List<string> _OECalculatedColumns = new List<string>();            
            OpenEdgeTableColumn c;
            OpenEdgeTable OETable = _table as OpenEdgeTable;
            OpenEdgeTableRow schema = OETable.SchemaRow as OpenEdgeTableRow;

            /*
            string[] separators = new string[] {" ", "+", "-", "*", "/", "(", ")"};
            string[] identifiers = s.Split(separators, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < identifiers.Length; i++)
            {
                _provider.debugOutput(0, string.Format("Identifier[{0}] = {1}",i, identifiers[i]));
            }
            // Check what an identifier is: something like a number or a Column.
            // In case of a column we have to know if this is a normal column or a calculated column. 
            for (int i=0;i<identifiers.Length;i++)
            {
                c = schema.GetColumn(identifiers[i].Trim()) as OpenEdgeTableColumn;
                if (c != null)
                {
                    if (c.OECalculatedColumn == true)
                    {
                        _OECalculatedColumns.Add(string.Format("{0}|{1}", c.OEColumnName, c.OEDataType));
                    }
                    else
                    {
                        _OEColumns.Add(c.OEColumnName);
                    }
                }
            }
            */
            for (int i = 0; i < args.UsedIdentifiers.Count; i++)
            {
                string[] parts = args.UsedIdentifiers[i].Split('.');
                int idx = parts.Length - 1;
                c = schema.GetColumn(parts[idx].Trim()) as OpenEdgeTableColumn;
                if (c != null)
                {
                    if (c.OECalculatedColumn == true)
                    {
                        _OECalculatedColumns.Add(string.Format("{0}|{1}", c.OEColumnName, c.OEDataType));
                    }
                    else
                    {
                        _OEColumns.Add(c.OEColumnName);
                    }
                }
            }

            OETableColumns = String.Join(",", _OEColumns.Distinct());
            OECalculatedColumns = String.Join(",", _OECalculatedColumns.Distinct());
        }

        public object Execute ()
        {
            bool success;
            object result;

            OELongchar jsonParameter = new OELongchar();
            _provider.ServiceParameter.AssignDefaultServiceParameter();
            jsonParameter.Data = _provider.ServiceParameter.GetJson();

            OELongchar jsonDataRequest = new OELongchar
            {
                Data = GetJson()
            };

            success = _provider.ServiceAdapter.GetData(_provider.ServiceName, jsonParameter, jsonDataRequest, out OELongchar jsonDataResponse);
            if (success)
            {
                result = ReadResponse(jsonDataResponse.Data);
                if (result != null && OEFunctionName == "COUNT")
                {
                    return Convert.ToInt32(result);
                }
                return result;
            }
            return null;
        }

        public string GetJson ()
        { 
            StringBuilder sb = new StringBuilder();
            JsonWriter w = new JsonWriter(sb)
            {
                PrettyPrint = true
            };
            try
            {
                // Request
                w.WriteObjectStart();
                w.WritePropertyName("OpenEdgeDataRequest");
                w.WriteObjectStart();

                // Table 
                w.WritePropertyName("OEDataTable");
                // Records 
                w.WriteArrayStart();

                // Record 
                int TableNumber = 1;
                w.WriteObjectStart();
                w.WritePropertyName("TableNumber");
                w.Write(TableNumber);
                w.WritePropertyName("OETableName");
                w.Write(OETableName);
                w.WritePropertyName("OEDbTableName");
                w.Write(OEDbTableName);
                w.WritePropertyName("OETableWhere");
                w.Write(OETableWhere);
                w.WritePropertyName("OETableColumns");
                w.Write(OETableColumns);
                w.WritePropertyName("OECalculatedColumns");
                w.Write(OECalculatedColumns);
                w.WriteObjectEnd();
                // End record

                w.WriteArrayEnd();
                // End records

                // Table 
                w.WritePropertyName("OEFunctionCall");
                // Records 
                w.WriteArrayStart();

                // Record 
                w.WriteObjectStart();
                w.WritePropertyName("OEFunctionCallGuid");
                w.Write(OEFunctionCallGuid);
                w.WritePropertyName("TableNumber");
                w.Write(TableNumber);
                w.WritePropertyName("OETableName");
                w.Write(OETableName);
                w.WritePropertyName("OEDbTableName");
                w.Write(OEDbTableName);
                w.WritePropertyName("OETableWhere");
                w.Write(OETableWhere);
                w.WritePropertyName("OETableColumns");
                w.Write(OETableColumns);
                w.WritePropertyName("OECalculatedColumns");
                w.Write(OECalculatedColumns);
                w.WritePropertyName("OEFunctionName");
                w.Write(OEFunctionName);
                w.WritePropertyName("OEExpression");
                w.Write(OEExpression);
                w.WritePropertyName("OEDistinct");
                w.Write(OEDistinct);
                w.WritePropertyName("Parameter1");
                w.Write(Parameter1);
                w.WritePropertyName("Parameter2");
                w.Write(Parameter2);
                w.WriteObjectEnd();
                // End record

                w.WriteArrayEnd();
                // End records

                w.WriteObjectEnd();
                w.WriteObjectEnd();
                // End request
            }
            catch (JsonException e)
            {
                _provider.DebugOutput(string.Format("** Json error while preparing request: {0}", e.Message));
                _provider.DebugOutput("Request so far:");
                _provider.DebugOutput(sb.ToString());
                return null;
            }
            return sb.ToString();
        }

        private object ReadResponse ( string json)
        {
            JsonReader reader = new JsonReader(json);
            bool Found = false;            

            while (reader.Read())
            {
                switch (reader.Token.ToString())
                {
                    case "PropertyName":
                        if (reader.Value.ToString() == "OEFunctionResult")
                        {
                            Found = true;
                        }
                        break;
                    default:
                        if (Found)
                        {
                            return reader.Value;                            
                        }
                        break;
                }
            }
            return null;
        }

        private string OpenEdgeFunctionName(NativeAggregateFunction function)
        {
            string OEName = "";
            switch (function)
            {
                case NativeAggregateFunction.Avg:
                    OEName = "AVERAGE";
                    break;
                case NativeAggregateFunction.Count:
                    OEName = "COUNT";
                    break;
                case NativeAggregateFunction.Max:
                    OEName = "MAXIMUM";
                    break;
                case NativeAggregateFunction.Min:
                    OEName = "MINIMUM";
                    break;
                case NativeAggregateFunction.Sum:
                    OEName = "TOTAL";
                    break;
            }
            return OEName;
        }
    }


    // Query and response container 
    internal class OpenEdgeQuery
    {
        OpenEdgeTable _table = null;
        ReadOnlyCollection<string> _identifiers = null;
        OpenEdgeDataProvider _provider = null;

        List<QueryTable> QueryTables = null;

        public OpenEdgeQuery(OpenEdgeTable table, ReadOnlyCollection<string> joinedidentifiers)
        {
            _table = table;
            _provider = _table.Provider;
            _identifiers = joinedidentifiers;
            QueryTables = new List<QueryTable>
            {
                new QueryTable(_table)
            };

            _provider.DebugOutput(string.Format("Table {0} - Identifiers", _table.TableName));
            foreach (string s in _identifiers)
            {
                _provider.DebugOutput(string.Format("-- {0}", s));
            }
            _provider.DebugOutput("-- Building query schema ..." );
            BuildSchema();
            _provider.DebugOutput("-- Query schema complete.");
        }

        public QueryTable GetQueryTableByOETableName(string OETableName)
        {
            foreach (QueryTable t in QueryTables)
            {
                if (t.Table.OETableName.ToLower() == OETableName.ToLower())
                {
                    return t;
                }
            }
            return null;
        }

        //CurrentFilter, RelationQuery, CurrentSort
        public OpenEdgeDataRequest BuildRequest(RelationQuery ParentRelationQuery, string Filter, List<OpenEdgeMultiValueFilter> MultiValueFilters, string SortOrder)
        {
            OpenEdgeDataRequest _request = new OpenEdgeDataRequest(_provider);
            QueryTable theTable;
            OEDataTable RequestTable;
            string op = "";
            string OETableWhere = "";

            if (ParentRelationQuery != null && ParentRelationQuery.OEChildWhere.Length > 0)
            {
                OETableWhere = String.Format("({0})", ParentRelationQuery.OEChildWhere);
                op = " AND ";
            }

            if (Filter.Length > 0)
            {
                OETableWhere = String.Format("{0}{1}({2})", OETableWhere, op, Filter);
            }

            // The Data Table
            theTable = QueryTables[0];
            string TableNameInRequest = _request.AddTable(theTable, theTable.Table.OETableName, theTable.Table.OEDbTableName, theTable.Table.OECalculatedTable, theTable.Table.OECachedTable, OETableWhere, SortOrder, theTable.UsedOETableColumns, theTable.UsedOECalculatedColumns);
            if (ParentRelationQuery != null)
            {
                RequestTable = _request.GetDataTableByOETableName(TableNameInRequest);
                RequestTable.OEParentTable      = ParentRelationQuery.OEParentTableName;
                RequestTable.OEParentTableWhere = ParentRelationQuery.OEParentWhere;
            }

            // Foreign Key Tables - recursive method!
            InsertForeignKeyTables(_request, _table);

            // Advanced filters 
            foreach(OpenEdgeMultiValueFilter m in MultiValueFilters)
            {
                 if (m.InOperator == true)
                    op = "IN";
                else
                    op = "NOT IN";
                _request.AddTableColumnFilter(m.OETableName, m.OEColumnName, m.OEDataType, m.FilterName, op, m.ValueDelimiter, m.Values);
            }
            return _request;
        }

        private void InsertForeignKeyTables(OpenEdgeDataRequest theRequest, OpenEdgeTable ChildTable)
        {
            // the ChildTable is the table where ForeignKeyTable theTable is referenced.
            // ChildTable <<-> ParentTable ( = ForeignKeyTable)
            foreach (QueryTable theTable in QueryTables)
            {
                if (theTable.ChildTable == ChildTable)
                {
                    OpenEdgeTable FKTable = theTable.Table;
                    OpenEdgeTableRelation FKRelation = theTable.ForeignKeyRelation;
                    OpenEdgeTableRow FKSchema = FKTable.SchemaRow as OpenEdgeTableRow;
                    OpenEdgeTableRow ChildSchema = ChildTable.SchemaRow as OpenEdgeTableRow;

                    // add the foreign key table to the request.
                    // If we have many instances of the same table (=Buffer) then we have to change then name now.
                    string TableNameInRequest = theRequest.AddTable(theTable, FKTable.OETableName, FKTable.OEDbTableName, theTable.Table.OECalculatedTable, theTable.Table.OECachedTable, "", "", theTable.UsedOETableColumns, theTable.UsedOECalculatedColumns);
                    theTable.OETableName = TableNameInRequest;
                    // build the relation fields for OpenEdge
                    string[] childcolumns  = FKRelation.ChildColumnName.Split('\t');
                    string[] parentcolumns = FKRelation.ParentColumnName.Split('\t');
                    string OERelationFields = "";
                    // The Relation ist a Child<<->Parent Relation in LL. 
                    // For the OpenEdge Join it's a Relation from the ChildTable to the FKTable 
                    // the Relationfields have reverse order 
                    for (int i = 0; i < childcolumns.Length; i++)
                    {
                        OpenEdgeTableColumn ChildColumn = ChildSchema.GetColumn(childcolumns[i]) as OpenEdgeTableColumn;
                        OpenEdgeTableColumn FKColumn    = FKSchema.GetColumn(parentcolumns[i]) as OpenEdgeTableColumn;
                        if (i > 0) OERelationFields += ",";
                        OERelationFields += String.Format("{0},{1}", ChildColumn.OEColumnName, FKColumn.OEColumnName);
                    }

                    // add OpenEdge Relation 
                    //theRequest.addRelation(ChildTable.OETableName, FKTable.OETableName, OERelationFields, FKRelation.RelationName);
                    theRequest.AddRelation(ChildTable.OETableName, theTable.OETableName, OERelationFields, FKRelation.RelationName);

                    // Let's see if we have a next level Foreign Key - recursive :-)
                    InsertForeignKeyTables(theRequest, FKTable);
                }
            }
        }

        private void BuildSchema()
        {
            OpenEdgeTableRelation _relation;
            foreach (string s in _identifiers)
            {
                QueryTable CurrentQueryTable = null;
                string[] parts = s.Split(':');
                _provider.DebugOutput(String.Format("--Identifier: {0} - Parts {1}", s, parts.Length));
                CurrentQueryTable = QueryTables[0];
                for (int p = 0; p < parts.Length; p++)
                {
                    _provider.DebugOutput(String.Format("  --Part[{0}]: {1} ", p, parts[p]));
                    if (parts[p].Contains('@'))
                    {
                        string ForeignKeyIdentifier;
                        if (p == 0)
                            ForeignKeyIdentifier = String.Format("{0}.{1}", _table.TableName, parts[p]);
                        else
                            ForeignKeyIdentifier = parts[p];

                        _relation = _provider.GetOpenEdgeFKRelation(ForeignKeyIdentifier);
                        if (_relation != null)
                        {
                            // Foreign Key Relation. Make sure that the relation parent fields are part of the response.
                            // They may not be part of list of identifiers.
                            CurrentQueryTable = EnsureForeignKeyQueryTable(CurrentQueryTable,_relation);
                            string[] columns = _relation.ParentColumnName.Split('\t');
                            for (int i = 0; i < columns.Length; i++)
                            {
                                CurrentQueryTable.AddColumn(columns[i]);
                            }
                            _provider.DebugOutput(String.Format("  --Part[{0}] > Relation {1} to QueryTable {2}", p, _relation.RelationName, CurrentQueryTable.Table.TableName));
                        }
                        else
                        {
                            _provider.DebugOutput(String.Format("  ** Foreign Key Relation {0} not found!", ForeignKeyIdentifier));
                        }
                    }
                    // Column of CurrentQueryTable
                    else
                    {
                        CurrentQueryTable.AddColumn(parts[p]);
                        _provider.DebugOutput(String.Format("  --Part[{0}] = Column {1} of QueryTable {2}", p, parts[p], CurrentQueryTable.Table.TableName));
                    }
                }
            }

            foreach (QueryTable FKTable in QueryTables)
            {
                // Last chance: Ensure that the relation child table has the columns for the fk relation
                if (FKTable.ForeignKeyRelation != null)
                {
                    string[] columns = FKTable.ForeignKeyRelation.ChildColumnName.Split('\t');
                    foreach (QueryTable ChildTable in QueryTables)
                    {
                        if (ChildTable.Table == FKTable.ChildTable)
                        {
                            for (int i = 0; i < columns.Length; i++)
                            {
                                ChildTable.AddColumn(columns[i]);
                            }
                            break;
                        }
                    }
                }
                _provider.DebugOutput(string.Format("Query Table: {0} - Columns: {1}", FKTable.OETableName, FKTable.UsedOETableColumns));
            }
        }

        private QueryTable EnsureForeignKeyQueryTable( QueryTable _childTable, OpenEdgeTableRelation _relation)
        {
            // It this Foreign Key already registered?
            foreach (QueryTable Table in QueryTables)
            {
                
                if ( Table.ChildQueryTable == _childTable && Table.ForeignKeyRelation.RelationIdentifier == _relation.RelationIdentifier)
                {
                    return Table;
                }
                
                /*
                if (Table.ForeignKeyRelation != null && Table.ForeignKeyRelation.ForeignKeyIdentifier == _relation.ForeignKeyIdentifier)
                {
                    return Table;
                }
                */
            }
            // Do we have a query table with the same name already?
            OpenEdgeTable TableInstance = _childTable.Table.Provider.GetTable(_relation.ParentTable.TableName) as OpenEdgeTable;
            QueryTable ForeignKeyTable = new QueryTable(TableInstance, _childTable, _relation);
            QueryTables.Add(ForeignKeyTable);
            return ForeignKeyTable;
        }
    }

    internal class QueryTable
    {        
        List<string> OEColumns;
        List<string> OECalculatedColumns;
        OpenEdgeTableRow _schema = null;

        public OpenEdgeTable Table { get; private set; }
        public OpenEdgeTable ChildTable { get; private set; }
        public QueryTable ChildQueryTable { get; private set; }

        public OpenEdgeTableRelation ForeignKeyRelation { get; private set; }
        public string OETableName { get; set; }

        public string UsedOETableColumns
        {
            get
            {
                return String.Join(",", OEColumns.Distinct());
            }
        }

        public string UsedOECalculatedColumns
        {
            get
            {
                return String.Join(",", OECalculatedColumns.Distinct());
            }
        }

        // Contructor for the Data Table. 
        public QueryTable( OpenEdgeTable table)
        {            
            Table = table;
            ChildTable = null;
            ForeignKeyRelation = null;
            _schema = Table.SchemaRow as OpenEdgeTableRow;
            OEColumns = new List<string>();
            OECalculatedColumns = new List<string>();
            OETableName = table.OETableName;
        }

        // Contructor for a Foreign Key Table 
        public QueryTable(OpenEdgeTable table, QueryTable childQueryTable, OpenEdgeTableRelation foreignkeyrelation)
        {            
            ForeignKeyRelation = foreignkeyrelation;

            Table = table;
            ChildQueryTable = childQueryTable;
            ChildTable = ChildQueryTable.Table;
            _schema = Table.SchemaRow as OpenEdgeTableRow;
            OEColumns = new List<string>();
            OECalculatedColumns = new List<string>();
            OETableName = Table.OETableName;
        }

        public void AddColumn(string name)
        {
            if (_schema.GetColumn(name) is OpenEdgeTableColumn column)
            {
                OEColumns.Add(column.OEColumnName);
                if (column.OECalculatedColumn == true)
                    OECalculatedColumns.Add(string.Format("{0}|{1}", column.OEColumnName, column.OEDataType));
            }
            else
                Table.Provider.DebugOutput(String.Format("  ** Column {0} not found!", name));
        }
    }


    // Catalog reader for Service definitions 
    internal class OpenEdgeServiceCatalogReader
    {
        JsonData Catalog;
        JsonData CatalogData;

        OpenEdgeDataProvider _provider;

        public void ReadSchema(OpenEdgeDataProvider provider, string servicename, string jsonString)
        {
            _provider = provider;
            CatalogData = JsonMapper.ToObject(jsonString);
            Catalog = CatalogData["OpenEdgeServiceCatalog"];
            ReadService(servicename, Catalog);
        }

        private void ReadService(string servicename, JsonData Catalog)
        {
            JsonData Service;
            string name;
            for (int i = 0; i < Catalog["OpenEdgeService"].Count; i++)
            {
                Service = Catalog["OpenEdgeService"][i];
                name = Service["ServiceName"].ToString();
                if (name.ToLower() == servicename.ToLower())
                {
                    try {ReadTables(Service);}
                    catch { }
                    try { ReadRelations(Service);}
                    catch { }
                    try { ReadViews(Service); }
                    catch { }
                }
            }
        }
        private void ReadTables(JsonData Service)
        {
            JsonData Table;
            OpenEdgeTable theTable;
            for (int i = 0; i < Service["OpenEdgeTable"].Count; i++)
            {
                Table = Service["OpenEdgeTable"][i];
                theTable = new OpenEdgeTable(_provider, Table["TableName"].ToString(), Table["OETableName"].ToString(), Table["OEDbTableName"].ToString(),
                                             Convert.ToBoolean(Table["OECalculatedTable"].ToString()), Convert.ToBoolean(Table["OECachedTable"].ToString()));
                ReadTableColumns(theTable, Table);
                _provider.AddTable(theTable);
            }
        }
        private void ReadTableColumns(OpenEdgeTable theTable, JsonData Table)
        {
            JsonData TableColumn;
            OpenEdgeTableRow SchemaRow;
            OpenEdgeTableColumn SchemaColumn;

            SchemaRow = new OpenEdgeTableRow(theTable);

            for (int i = 0; i < Table["OpenEdgeTableColumn"].Count; i++)
            {
                TableColumn = Table["OpenEdgeTableColumn"][i];
                SchemaColumn = new OpenEdgeTableColumn(theTable,
                                                       TableColumn["ColumnName"].ToString(),
                                                       TableColumn["OEColumnName"].ToString(),
                                                       Convert.ToInt32(TableColumn["OEColumnIndex"].ToString()),
                                                       TableColumn["OEColumnDataType"].ToString(),
                                                       TableColumn["OEMimeType"].ToString(),
                                                       TableColumn["OESampleValue"].ToString(),
                                                       Convert.ToBoolean(TableColumn["OECalculatedColumn"].ToString()),
                                                       Convert.ToBoolean(TableColumn["OECaseSensitive"].ToString())
                                                       );
                SchemaRow.AddColumn(SchemaColumn);
            }
            theTable.SchemaRow = SchemaRow;

        }
        private void ReadRelations(JsonData Service)
        {
            JsonData Relation;
            OpenEdgeTableRelation theRelation;

            for (int i = 0; i < Service["OpenEdgeDataRelation"].Count; i++)
            {
                Relation = Service["OpenEdgeDataRelation"][i];
                theRelation = new OpenEdgeTableRelation(_provider, Relation["ParentTableName"].ToString(), Relation["ChildTableName"].ToString(), Relation["ParentColumnName"].ToString(), Relation["ChildColumnName"].ToString(), Relation["RelationName"].ToString());
                _provider.AddRelation(theRelation);
            }
        }
        private void ReadViews(JsonData Service)
        {
            JsonData View;
            OpenEdgeView theView;

            for (int i = 0; i < Service["OpenEdgeView"].Count; i++)
            {
                View = Service["OpenEdgeView"][i];
                theView = new OpenEdgeView(View["ViewName"].ToString(), View["ViewTables"].ToString(), View["ViewRelations"].ToString());
                _provider.AddView(theView);
            }
        }

    }

    // Data reader for OpenEdge json data.
    // This data has to be in a special format.
    internal class OpenEdgeResponseReader
    {
        //JsonData Response;
        //JsonData Data;        
        OpenEdgeDataProvider _provider = null;        
        OpenEdgeDataRequest _request;

        public OpenEdgeResponseReader(OpenEdgeDataProvider provider, string response, bool fullresponse)
        {
            _provider = provider;
            // fullresponse = true means that we got the data for an entire dataset. 
            // fullresponse = false means that we got single (child) table data including foreign keys.
            // the foreign key tables have to be handled different.            
            ReadResponse(response);
        }

        public OpenEdgeResponseReader(OpenEdgeDataProvider provider, OpenEdgeDataRequest theRequest, string response)
        {
            _provider = provider;
            _request = theRequest;
            // fullresponse = false means that we got single (child) table data including foreign keys.
            // the foreign key tables have to be handled different.            
            ReadResponse(response);
        }

        private void ReadResponse(string json)
        {
            JsonReader reader = new JsonReader(json);

            int ObjectLevel = 0;
            int ArrayLevel = 0;
            string oecolumnname = "";
            int oecolumnindex = 0;
            int record = 0;

            OpenEdgeTable CurrentTable = null;
            OpenEdgeTableRow CurrentRow = null;
            QueryTable CurrentQueryTable = null;

            bool isQuery = false;
            if (_request != null) isQuery = true;

            /*
               ObjectLevel Arraylevel PropertyIs       Comment
               ----------- ---------- ---------------- ---------------------------------------------
                    1          0       DatasetName     Fixed "OpenEdgeDataResponse"
                    2          0       TableName       First Token
                    2          1                       Record Array for Table <TableName>
                    3          1       ColumnName      Record for Table <TableName>
                    3          2                       Array with values for an OpenEdge Array field

            */
            while (reader.Read())
            {
                _ = reader.Value != null ?
                    reader.Value.GetType().ToString() : "";

                switch (reader.Token.ToString())
                {
                    case "ObjectStart":
                        ObjectLevel++;
                        oecolumnindex = 0;
                        if (ObjectLevel == 1)
                        {
                            // Dataset
                        }
                        else if (ObjectLevel == 2)
                        {
                            // Table
                            record = 0;
                        }
                        else
                        {
                            // Record 
                            record++;
                            CurrentRow = new OpenEdgeTableRow(CurrentTable);
                        }
                        break;
                    case "ArrayStart":
                        oecolumnindex = 0; // 20161108 - fix for multiple array in one record 
                        ArrayLevel++;
                        break;
                    case "ObjectEnd":
                        if (ObjectLevel == 1)
                        {
                        }
                        else if (ObjectLevel == 2)
                        {
                        }
                        else
                        {
                            if (isQuery)
                            {
                                // Since LL26: if the table is a foreign key then add the row to the foreign key cache inside the child table instance
                                if (CurrentQueryTable.ForeignKeyRelation != null)
                                {
                                    //CurrentQueryTable.ForeignKeyRelation.AddForeignKey(CurrentRow);
                                    CurrentQueryTable.ChildTable.AddOpenEdgeForeignTableRow(CurrentQueryTable.ForeignKeyRelation,CurrentRow);
                                }
                                else
                                {
                                    CurrentTable.AddOpenEdgeTableRow(CurrentRow);
                                }
                            }
                            else
                            {
                                CurrentTable.AddOpenEdgeTableRow(CurrentRow);
                            }
                        }
                        ObjectLevel--;
                        break;
                    case "ArrayEnd":
                        ArrayLevel--;
                        break;
                    case "PropertyName":
                        if (ObjectLevel == 1)
                        {
                            // The datasetname must match !!
                            string oedatasetname = reader.Value.ToString();
                            if ( string.Compare(oedatasetname,"OpenEdgeDataResponse", true) != 0 )
                            {
                                return;
                            }
                        }
                        else if (ObjectLevel == 2)
                        {
                            string oetablename = reader.Value.ToString();
                            oecolumnname = "";
                            // START Table
                            if (isQuery)
                            {
                                CurrentQueryTable = _request.GetQueryTableByOETableName(oetablename);
                                CurrentTable = CurrentQueryTable.Table;
                            }
                            else
                            {
                                //CurrentTable = _provider.GetOpenEdgeTable(oetablename);
                                CurrentQueryTable = _request.GetQueryTableByOETableName(oetablename);
                                CurrentTable = CurrentQueryTable.Table;
                            }
                        }
                        else
                        {
                            oecolumnname = reader.Value.ToString();
                        }
                        break;

                    // We read a value: Token = Json DataType, Value = Value, Type is systemtype.
                    default:
                        string llcolumnname;
                        // If the array level is 2 we are inside the values of an OpenEdge Array.
                        if (ArrayLevel == 2)
                        { oecolumnindex++; llcolumnname = oecolumnname + "_" + oecolumnindex.ToString(); }
                        else
                        // We have a value for a normal column
                        { oecolumnindex = 0; llcolumnname = oecolumnname; }
                        //Column = new OpenEdgeTableColumn(CurrentTable, llcolumnname, reader.Value.GetType(), reader.Value);
                        OpenEdgeTableColumn Column = new OpenEdgeTableColumn(CurrentTable, llcolumnname, reader.Value);

                        CurrentRow.AddColumn(Column);
                        break;
                }
            }
        }
    }

    internal class OpenEdgeServiceParameter
    {
        private Hashtable ServiceParameter = null;
        private OpenEdgeDataProvider _provider = null;

        public OpenEdgeServiceParameter(OpenEdgeDataProvider provider)
        {
            _provider = provider;
            ServiceParameter = new Hashtable();
        }

        public void AssignDefaultServiceParameter ()
        {
            SetParameterValue("ViewName", _provider.ViewName);
            SetParameterValue("ClientCulture", CultureInfo.CurrentCulture.Name);
            SetParameterValue("UseInvariantCulture", _provider.UseInvariantCulture);
            SetParameterValue("MaxRows", _provider.MaxRows);
            /* 20180925 */
            SetParameterValue("ClientId",_provider.ClientId);
        }

        public void SetParameterValue (string name, object value)
        {
            if (ServiceParameter.ContainsKey(name))
            {
                ServiceParameter[name] = value;
            }
            else
            {
                ServiceParameter.Add(name, value);            }
        }

        public string GetJson()
        {
            StringBuilder sb = new StringBuilder();
            JsonWriter w = new JsonWriter(sb)
            {
                //CultureInfo ci = new CultureInfo("en-US");
                PrettyPrint = true
            };
            try
            {
                w.WriteObjectStart();
                w.WritePropertyName("OpenEdgeServiceParameter");
                w.WriteObjectStart();
                if (ServiceParameter.Count > 0)
                {
                    w.WritePropertyName("OEServiceParameter");
                    w.WriteArrayStart();
                    foreach (string name in ServiceParameter.Keys)
                    {
                        w.WriteObjectStart();
                        w.WritePropertyName("OEParameterName");
                        w.Write(name);
                        w.WritePropertyName("OEParameterDataType");
                        w.Write(ServiceParameter[name].GetType().ToString());
                        w.WritePropertyName("OEParameterValue");
                        //w.Write(_provider.OEStringValue(ServiceParameter[name]));
                        w.Write(_provider.OEStringValue(ServiceParameter[name]));
                        //w.Write(String.Format("{0}", ServiceParameter[name]));

                        w.WriteObjectEnd();
                    }
                    w.WriteArrayEnd();
                }
                w.WriteObjectEnd();
                w.WriteObjectEnd();
            }
            catch
            {
                return "";
            }
            return sb.ToString();
        }
    }

    // The data request for OpenEdge
    // It's just there to be able to generate the Json request.
    internal class OpenEdgeDataRequest
    {
        int _tableNumber = 0;
        List<OEDataTable> _tables = null;
        List<OEDataRelation> _relations = null;
        List<OEAdvancedFilter> _filters = null;
        OpenEdgeDataProvider _provider = null;

        public OpenEdgeDataRequest(OpenEdgeDataProvider Provider)
        {
            _provider = Provider;
            _tables = new List<OEDataTable>();
            _relations = new List<OEDataRelation>();
            _filters = new List<OEAdvancedFilter>();
        }

        public string AddTable(QueryTable QueryTable, string OETableName, string OEDbTableName, bool OECalculatedTable, bool _/*OECachedTable*/, string OETableWhere, string OETableSortBy, string OETableColumns, string OECalculatedColumns)
        {
            OEDataTable _table = new OEDataTable
            {
                QueryTable = QueryTable
            };
            _tableNumber++;

            string BufferName = OETableName;

            // check if the same table already exists in the request.
            // Then we have to generate a new buffername for OE. 
            foreach (OEDataTable Instance in _tables)
            {
                if (Instance.OETableName == OETableName)
                {
                    string numberString = String.Format("{0}", _tableNumber);
                    // length must be <= 32 
                    if ( OETableName.Length + numberString.Length > 32)
                    {
                        BufferName = BufferName.Substring(1, 32 - numberString.Length);
                    }
                    BufferName = string.Format("{0}{1}", BufferName, _tableNumber);
                    break;
                }
            }

            _table.TableNumber          = _tableNumber;
            _table.OETableName          = BufferName;
            _table.OEDbTableName        = OEDbTableName;
            _table.OETableWhere         = OETableWhere;
            _table.OETableSortBy        = OETableSortBy;
            _table.OETableColumns       = OETableColumns;
            _table.OECalculatedTable    = OECalculatedTable;
            _table.OECalculatedColumns  = OECalculatedColumns;
            _tables.Add(_table);
            return _table.OETableName;
        }

        public void AddForeignKeyTable(QueryTable QueryTable,  string OETableName, string OEDbTableName, string OETableColumns)
        {
            OEDataTable _table = new OEDataTable
            {
                QueryTable = QueryTable,
                TableNumber = _tableNumber,
                OETableName = OETableName,
                OEDbTableName = OEDbTableName,
                OETableColumns = OETableColumns
            };
            _tables.Add(_table);
        }

        public void AddTableColumnFilter (string OETableName, string OEColumnName, string OEDataType, string OEFilterName, string OEFilterOperator, string OEValueDelimiter ,string OEFilterValues)
        {
            OEAdvancedFilter _filter = new OEAdvancedFilter
            {
                OETableName = OETableName,
                OEColumnName = OEColumnName,
                OEDataType = OEDataType,
                OEFilterName = OEFilterName,
                OEFilterOperator = OEFilterOperator,
                OEValueDelimiter = OEValueDelimiter,
                OEFilterValues = OEFilterValues
            };
            _filters.Add(_filter);
         }

        public void AddRelation(string OETableName, string OEChildTableName, string OERelationFields, string OERelationName)
        {
            OEDataRelation _relation = new OEDataRelation
            {
                OETableName = OETableName,
                OEChildTableName = OEChildTableName,
                OERelationName = OERelationName,
                OERelationFields = OERelationFields
            };
            _relations.Add(_relation);
        }

        public QueryTable GetQueryTableByOETableName(string OETableName)
        {
            foreach(OEDataTable DataTable in _tables)
            {
                if (DataTable.OETableName == OETableName)
                    return DataTable.QueryTable;
            }
            return null;
        }

        public OEDataTable GetDataTableByOETableName(string OETableName)
        {
            foreach (OEDataTable DataTable in _tables)
            {
                if (DataTable.OETableName == OETableName)
                    return DataTable;
            }
            return null;
        }

        public string GetJson()
        {
            StringBuilder sb = new StringBuilder();
            JsonWriter w = new JsonWriter(sb)
            {
                PrettyPrint = true
            };
            try
            {
                // Request
                w.WriteObjectStart();
                w.WritePropertyName("OpenEdgeDataRequest");
                w.WriteObjectStart();

                // OEDataTable records 
                if (_tables.Count > 0)
                {
                    w.WritePropertyName("OEDataTable");
                    w.WriteArrayStart();
                    foreach (OEDataTable _table in _tables)
                    {
                        w.WriteObjectStart();
                        w.WritePropertyName("TableNumber");
                        w.Write(_table.TableNumber);
                        w.WritePropertyName("OETableName");
                        w.Write(_table.OETableName);
                        w.WritePropertyName("OEDbTableName");
                        w.Write(_table.OEDbTableName);
                        w.WritePropertyName("OECalculatedTable");
                        w.Write(_table.OECalculatedTable);
                        w.WritePropertyName("OECachedTable");
                        w.Write(_table.OECachedTable);
                        w.WritePropertyName("OETableWhere");
                        w.Write(_table.OETableWhere);
                        w.WritePropertyName("OETableSortBy");
                        w.Write(_table.OETableSortBy);
                        w.WritePropertyName("OETableColumns");
                        w.Write(_table.OETableColumns);
                        w.WritePropertyName("OECalculatedColumns");
                        w.Write(_table.OECalculatedColumns);
                        w.WritePropertyName("OEParentTableName");
                        w.Write(_table.OEParentTable);
                        w.WritePropertyName("OEParentTableWhere");
                        w.Write(_table.OEParentTableWhere);
                        w.WriteObjectEnd();
                    }
                    w.WriteArrayEnd();
                }

                // OEDataRelation records 
                if (_relations.Count > 0)
                {
                    w.WritePropertyName("OEDataRelation");
                    w.WriteArrayStart();
                    foreach (OEDataRelation _relation in _relations)
                    {
                        w.WriteObjectStart();
                        w.WritePropertyName("OETableName");
                        w.Write(_relation.OETableName);
                        w.WritePropertyName("OEChildTableName");
                        w.Write(_relation.OEChildTableName);
                        w.WritePropertyName("OERelationName");
                        w.Write(_relation.OERelationName);
                        w.WritePropertyName("OERelationFields");
                        w.Write(_relation.OERelationFields);
                        w.WriteObjectEnd();
                    }
                    w.WriteArrayEnd();
                }

                // OETableColumnFilter records 
                if (_filters.Count > 0)
                {
                    w.WritePropertyName("OEAdvancedFilter");
                    w.WriteArrayStart();
                    foreach (OEAdvancedFilter _filter in _filters)
                    {
                        w.WriteObjectStart();
                        w.WritePropertyName("OETableName");
                        w.Write(_filter.OETableName);
                        w.WritePropertyName("OEColumnName");
                        w.Write(_filter.OEColumnName);
                        w.WritePropertyName("OEDataType");
                        w.Write(_filter.OEDataType);
                        w.WritePropertyName("OEFilterName");
                        w.Write(_filter.OEFilterName);
                        w.WritePropertyName("OEFilterOperator");
                        w.Write(_filter.OEFilterOperator);
                        w.WritePropertyName("OEValueDelimiter");
                        w.Write(_filter.OEValueDelimiter);
                        w.WritePropertyName("OEFilterValues");
                        w.Write(_filter.OEFilterValues);
                        w.WriteObjectEnd();
                    }
                    w.WriteArrayEnd();
                }
                w.WriteObjectEnd();
                w.WriteObjectEnd();
            }
            catch (JsonException e)
            {
                _provider.DebugOutput(string.Format("** Json error while preparing request: {0}", e.Message));
                _provider.DebugOutput("Request so far:");
                _provider.DebugOutput(sb.ToString());
                return null;
            }
            return sb.ToString();
        }
    }

    internal class OEDataTable
    {
        public int TableNumber { get; set; }
        public QueryTable QueryTable { get; set; }
        public string OETableName { get; set; }
        public string OEDbTableName { get; set; }
        public bool   OECalculatedTable { get; set; }
        public bool   OECachedTable { get; set; }
        public string OETableWhere { get; set; }
        public string OETableSortBy { get; set; }
        public string OETableColumns { get; set; }
        public string OECalculatedColumns { get; set; }
        // Since LL24 - references a parent row for calculated tables
        public string OEParentTable { get; set; } = "";
        public string OEParentTableWhere { get; set; } = "";

    }

    internal class OEAdvancedFilter
    {
        public string OETableName      { get; set; }
        public string OEColumnName     { get; set; }
        public string OEDataType       { get; set; }
        public string OEFilterName     { get; set; }
        public string OEFilterOperator { get; set; }
        public string OEValueDelimiter { get; set; }
        public string OEFilterValues   { get; set; }
    }

    internal class OEDataRelation
    {
        public string OETableName { get; set; }
        public string OEChildTableName { get; set; }
        public string OERelationName { get; set; }
        public string OERelationFields { get; set; }
    }

    // The good part - An OpenEdge Adapter Interface 
    // - implemented in C# i.e. for a .Net Client like List & Label Server
    // - implemented in OpenEdge for OpenEdge Clients.
    // - since LL24 ClientEvent
    public interface IServiceAdapter
    {
        bool GetSchema   (string ServiceName, OELongchar JsonServiceParameter, out OELongchar JsonSchema);
        bool GetData     (string ServiceName, OELongchar JsonServiceParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse);
        bool ClientEvent (string ServiceName, OELongchar JsonServiceParameter, OELongchar JsonDataRequest, out OELongchar JsonDataResponse);
    }

    // Container for a json string passed as parameter to and from OpenEdge.
    public class OELongchar
    {
        public string Data { get; set; }
    }

    // since LL24 - Client Event 
    internal class OpenEdgeClientEvent
    {
        public string ClientId  { get; set; }
        public string EventName { get; set; }
        string EventId = Guid.NewGuid().ToString();

        public OpenEdgeClientEvent (string clientid, string eventname)
        {
            ClientId  = clientid;
            EventName = eventname;
        }

        public string GetJson()
        {
            _ = Guid.NewGuid().ToString();
            StringBuilder sb = new StringBuilder();
            JsonWriter w = new JsonWriter(sb)
            {
                PrettyPrint = true
            };
            try
            {
                w.WriteObjectStart();
                // Dataset 
                w.WritePropertyName("OpenEdgeClientEvent");
                    w.WriteObjectStart();
                
                        // Temp-Table 
                        w.WritePropertyName("OEClientEvent");
                        w.WriteArrayStart();

                        // Temp-Table fields 
                        w.WriteObjectStart();
                        w.WritePropertyName("EventId");
                        w.Write(EventId);
                        w.WritePropertyName("EventName");
                        w.Write(EventName);
                        w.WritePropertyName("ClientId");
                        w.Write(ClientId);
                        w.WriteObjectEnd();

                        w.WriteArrayEnd();

                    w.WriteObjectEnd();
                w.WriteObjectEnd();
            }
            catch
            {
                return "";
            }
            return sb.ToString();
        }
    }
}
