using System;
using System.ComponentModel;
using System.Data;
using System.Data.OleDb;
using System.Net;
using System.Runtime.Serialization;
using System.Security;

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Provides a data provider implementation for Microsoft Access databases.
    /// </summary>
    /// <remarks>
    /// The <see cref="AccessDataProvider"/> class extends <see cref="OleDbConnectionDataProvider"/> to provide connectivity to 
    /// Microsoft Access databases (both .mdb and .accdb files). It supports advanced filtering and uses Microsoft Access SQL syntax 
    /// for query generation. The provider automatically detects whether to use the Microsoft ACE OLEDB provider (preferred) or 
    /// the Jet OLEDB provider (fallback) based on the system configuration and the installed drivers.
    /// </remarks>
    /// <example>
    /// The following example demonstrates how to use the <see cref="AccessDataProvider"/>:
    /// <code language="csharp">
    /// // Create an instance of the AccessDataProvider for an Access database file.
    /// AccessDataProvider provider = new AccessDataProvider(@"C:\Data\MyDatabase.accdb", "mypassword");
    /// 
    /// // Assign the provider as the data source for the List &amp; Label reporting engine.
    /// using ListLabel listLabel = new ListLabel();
    /// listLabel.DataSource = provider;
    /// ExportConfiguration exportConfiguration = new ExportConfiguration(LlExportTarget.Pdf, exportFilePath, projectFilePath);
    /// exportConfiguration.ShowResult = true;
    /// listLabel.Export(exportConfiguration);
    /// </code>
    /// </example>
    [Serializable]
    public sealed class AccessDataProvider : OleDbConnectionDataProvider
    {
        /// <summary>
        /// Gets the file name of the Access database.
        /// </summary>
        public string FileName { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AccessDataProvider"/> class for the specified Access database file.
        /// </summary>
        /// <param name="fileName">The path to the Access database file.</param>
        public AccessDataProvider(string fileName)
            : this(fileName, String.Empty)
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="AccessDataProvider"/> class for the specified Access database file using the provided password.
        /// </summary>
        /// <param name="fileName">The path to the Access database file.</param>
        /// <param name="password">The password for the Access database.</param>
        public AccessDataProvider(string fileName, string password)
            : base(GetOleDbConnection(fileName, password))
        {
            FileName = fileName;
            SupportsAdvancedFiltering = true;
            base.UseMsAccessSqlSyntax = true;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AccessDataProvider"/> class from serialized data.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> containing the serialized object data.</param>
        /// <param name="context">The <see cref="StreamingContext"/> that contains contextual information about the source or destination.</param>
        private AccessDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            FileName = info.GetString("AccessFilePath");
            base.UseMsAccessSqlSyntax = true;
        }

        /// <summary>
        /// Populates a <see cref="SerializationInfo"/> with the data needed to serialize the <see cref="AccessDataProvider"/>.
        /// </summary>
        /// <param name="info">The <see cref="SerializationInfo"/> to populate with data.</param>
        /// <param name="context">The destination for this serialization.</param>
        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("AccessFilePath", FileName);
        }

        /// <summary>
        /// Creates and returns an <see cref="OleDbConnection"/> for the specified Access database file.
        /// </summary>
        /// <param name="path">The path to the Access database file.</param>
        /// <param name="dbPassword">The password for the database (if any).</param>
        /// <returns>An <see cref="OleDbConnection"/> object configured to connect to the specified database.</returns>
        /// <exception cref="ListLabelException">
        /// Thrown if the required Microsoft Access Database Engine is not installed on a 64-bit system.
        /// </exception>
        private static OleDbConnection GetOleDbConnection(string path, string dbPassword)
        {
            // Check if the Microsoft ACE OLEDB provider is installed.
            OleDbEnumerator enumerator = new OleDbEnumerator();
            DataTable table = enumerator.GetElements();
            bool aceFound = false;

            foreach (DataRow row in table.Rows)
            {
                foreach (DataColumn col in table.Columns)
                {
                    if ((col.ColumnName.Contains("SOURCES_NAME")) && (row[col].ToString().Contains("Microsoft.ACE.OLEDB.12.0")))
                    {
                        aceFound = true;
                        break;
                    }
                }

                if (aceFound)
                    break;
            }

            string connectionString = "Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};Jet OLEDB:Database Password={1};Persist Security Info=False;";
            if (aceFound) // Use ACE 12.0 provider.
            {
                connectionString = String.Format(connectionString, path, dbPassword);
            }
            else if (!aceFound && IntPtr.Size == 4) // Use Jet 4.0 provider for 32-bit systems.
            {
                connectionString = String.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};Jet OLEDB:Database Password={1};", path, dbPassword);
            }
            else
            {
                throw new ListLabelException("Please install the x64 Microsoft's Access Database Engine");
            }

            return new OleDbConnection(connectionString);
        }
    }
}