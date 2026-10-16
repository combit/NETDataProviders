using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
#if NET_BUILD
using Microsoft.Data.SqlClient;
#else
using System.Data.SqlClient;
#endif

using System.Runtime.Serialization;
using System.Text;
using System.ComponentModel;

namespace combit.Reporting.DataProviders
{
    /// <summary>
    /// Authentication type
    /// </summary>
    public enum AzureSqlDataProviderAuthenticationType
    {
        /// <summary>
        /// Use integrated windows authentication
        /// </summary>
        IntegratedWindowsAuthentication,
        /// <summary>
        /// Use an Azure AD identity, i. e. username and password
        /// </summary>
        AzureADIdentity
    }

    /// <summary>
    /// Provides the configuration data for connecting to Azure SQL databases.
    /// </summary>
    /// <remarks>
    /// The <see cref="AzureSqlDataProviderConfiguration"/> class encapsulates all connection parameters needed to connect
    /// to Microsoft Azure SQL Database. It supports two authentication methods: Azure Active Directory Integrated authentication
    /// (for domain-joined machines) and Azure AD username/password authentication. The class automatically generates a properly
    /// formatted Azure SQL connection string with security best practices, including encryption enabled by default. You can
    /// also override the generated connection string with a custom one using the <see cref="ConnectionStringOverride"/> property.
    /// </remarks>
    public class AzureSqlDataProviderConfiguration
    {
        /// <summary>
        /// The server name, e. g. contoso.database.windows.net
        /// </summary>
        public string Server { get; set; }
        /// <summary>
        /// The database name, e. g. Northwind
        /// </summary>
        public string Database { get; set; }
        /// <summary>
        /// Enable multiple active result sets (MARS)
        /// </summary>
        public bool MultipleActiveResultSets { get; set; }
        /// <summary>
        /// Enable encryption
        /// </summary>
        public bool Encrypt { get; set; }
        /// <summary>
        /// Trust the server's certificate
        /// </summary>
        public bool TrustServerCertificate { get; set; }
        /// <summary>
        /// The port of the database, default is 1433
        /// </summary>
        public int Port { get; set; }
        /// <summary>
        /// The authentication type, see <see cref="AzureSqlDataProviderAuthenticationType"/>
        /// </summary>
        public AzureSqlDataProviderAuthenticationType AuthenticationType { get; set; }
        /// <summary>
        /// Enables to override the automatically generated connection string with a custom one
        /// </summary>
        public string ConnectionStringOverride { get; set; }
        /// <summary>
        /// The user name if AuthenticationType is set to <see cref="AzureSqlDataProviderAuthenticationType.AzureADIdentity"/>
        /// </summary>
        public string UserName { get; set; }
        /// <summary>
        /// The password if AuthenticationType is set to <see cref="AzureSqlDataProviderAuthenticationType.AzureADIdentity"/>
        /// </summary>
        public String Password { get; set; }

        /// <summary>
        /// Constructs an instance of the AzureSqlDataProviderConfiguration
        /// </summary>
        /// <param name="server">The server name, e. g. contoso.database.windows.net</param>
        /// <param name="database">The database name, e. g. Northwind</param>
        /// <param name="authenticationType">The authentication type, see <see cref="AzureSqlDataProviderAuthenticationType"/> </param>
        public AzureSqlDataProviderConfiguration(string server, string database, AzureSqlDataProviderAuthenticationType authenticationType)
        {
            Server = server;
            Database = database;
            MultipleActiveResultSets = false;
            Encrypt = true;
            TrustServerCertificate = false;
            AuthenticationType = authenticationType;
            Port = 1433;
        }

        internal string GetConnectionString()
        {
            if (!string.IsNullOrEmpty(ConnectionStringOverride))
            {
                return ConnectionStringOverride;
            }

            if (AuthenticationType == AzureSqlDataProviderAuthenticationType.AzureADIdentity && (string.IsNullOrEmpty(UserName) || string.IsNullOrEmpty(Password)))
            {
                throw new ListLabelException("Please assign a user name and password for AzureADIdentity authentication.");
            }

            StringBuilder connectionString = new StringBuilder();
            connectionString.Append("Server=tcp:");
            connectionString.Append(String.Format($"{Server},{Port};"));
            connectionString.Append(String.Format($"Initial Catalog=\"{Database}\";"));

            if (AuthenticationType == AzureSqlDataProviderAuthenticationType.AzureADIdentity)
            {
                connectionString.Append(String.Format($"User ID={UserName};Password=\"{Password.Replace("\"", "\"\"")}\";"));
                connectionString.Append("Authentication=\"Active Directory Password\";");
            }
            else
            {
                connectionString.Append("Authentication=\"Active Directory Integrated\";");
            }

            connectionString.Append(String.Format("MultipleActiveResultSets={0};", MultipleActiveResultSets ? "True" : "False"));
            connectionString.Append(String.Format("Encrypt={0};", Encrypt ? "True" : "False"));
            connectionString.Append(String.Format("TrustServerCertificate={0};", TrustServerCertificate ? "True" : "False"));
            connectionString.Append("Persist Security Info=False;");
            return connectionString.ToString();
        }
    }

    /// <summary>
    /// Provides a data provider for connecting to Microsoft Azure SQL databases.
    /// </summary>
    /// <remarks>
    /// The <see cref="AzureSqlDataProvider"/> class extends <see cref="SqlConnectionDataProvider"/> to provide specialized
    /// connectivity to Azure SQL Database, Microsoft's cloud-based relational database service. It simplifies authentication
    /// by supporting both Azure Active Directory Integrated authentication and Azure AD username/password authentication.
    /// The provider uses an <see cref="AzureSqlDataProviderConfiguration"/> object to configure connection parameters including
    /// server address, database name, authentication type, encryption settings, and Multiple Active Result Sets (MARS) support.
    /// All features of <see cref="SqlConnectionDataProvider"/> are available, including tables, views, advanced filtering,
    /// schema-based filtering, and automatic discovery of foreign key relationships. The provider automatically constructs
    /// Azure SQL-specific connection strings with proper security settings and supports schema filtering to limit data access
    /// to specific database schemas.
    /// </remarks>
    /// <example>
    /// The following example demonstrates how to use the <see cref="AzureSqlDataProvider"/> to export a report to PDF:
    /// <code language="csharp">
    /// // Example 1: Using Azure AD Integrated Authentication (for domain-joined machines)
    /// AzureSqlDataProviderConfiguration config = new AzureSqlDataProviderConfiguration(
    ///     "myserver.database.windows.net",
    ///     "mydatabase",
    ///     AzureSqlDataProviderAuthenticationType.IntegratedWindowsAuthentication);
    /// 
    /// // Optional: Configure connection settings
    /// config.Encrypt = true; // Enabled by default
    /// config.MultipleActiveResultSets = false;
    /// config.Port = 1433; // Default Azure SQL port
    /// 
    /// // Create the AzureSqlDataProvider
    /// AzureSqlDataProvider provider = new AzureSqlDataProvider(config);
    /// 
    /// // Create a ListLabel reporting engine instance and assign the provider as the data source.
    /// using ListLabel listLabel = new ListLabel();
    /// listLabel.DataSource = provider;
    /// 
    /// // Configure export settings to generate a PDF.
    /// ExportConfiguration exportConfiguration = new ExportConfiguration(LlExportTarget.Pdf, exportFilePath, projectFilePath);
    /// exportConfiguration.ShowResult = true;
    /// 
    /// // Export the report to PDF.
    /// listLabel.Export(exportConfiguration);
    /// 
    /// // Example 2: Using Azure AD Identity (username and password)
    /// AzureSqlDataProviderConfiguration adConfig = new AzureSqlDataProviderConfiguration(
    ///     "myserver.database.windows.net",
    ///     "mydatabase",
    ///     AzureSqlDataProviderAuthenticationType.AzureADIdentity);
    /// 
    /// adConfig.UserName = "user@domain.com";
    /// adConfig.Password = "your_password_here";
    /// 
    /// AzureSqlDataProvider adProvider = new AzureSqlDataProvider(adConfig);
    /// 
    /// // Example 3: Filtering by specific schema
    /// AzureSqlDataProvider schemaProvider = new AzureSqlDataProvider(config, "dbo");
    /// 
    /// // Example 4: Using custom connection string override
    /// AzureSqlDataProviderConfiguration customConfig = new AzureSqlDataProviderConfiguration(
    ///     "myserver.database.windows.net",
    ///     "mydatabase",
    ///     AzureSqlDataProviderAuthenticationType.IntegratedWindowsAuthentication);
    /// 
    /// customConfig.ConnectionStringOverride = "your custom connection string here";
    /// AzureSqlDataProvider customProvider = new AzureSqlDataProvider(customConfig);
    /// </code>
    /// </example>
    [Serializable]
    public class AzureSqlDataProvider : SqlConnectionDataProvider
    {
        private AzureSqlDataProvider() { }
        /// <summary>
        /// Constructs an instance of the AzureSqlDataProvider
        /// </summary>
        /// <param name="configuration">The configuration to use</param>
        public AzureSqlDataProvider(AzureSqlDataProviderConfiguration configuration)
        {
            Connection = new SqlConnection();
            Connection.ConnectionString = configuration.GetConnectionString();

            SupportedElementTypes = DbConnectionElementTypes.Table | DbConnectionElementTypes.View;
            PrefixTableNameWithSchema = false;
            SupportsAdvancedFiltering = true;

            List<string> list = new List<string>();
            TableSchemas = list.AsReadOnly();
            InitSqlModifications();
        }

        /// <summary>
        /// Constructs an instance of the AzureSqlDataProvider
        /// </summary>
        /// <param name="configuration">The configuration to use</param>
        /// <param name="tableSchema">The provided data will be restricted to this schema</param>
        public AzureSqlDataProvider(AzureSqlDataProviderConfiguration configuration, string tableSchema)
            : this(configuration)
        {
            if (!string.IsNullOrEmpty(tableSchema))
            {
                List<string> list = new List<string>();
                list.Add(tableSchema);
                TableSchemas = list.AsReadOnly();
            }
        }

        /// <summary>
        /// Constructs an instance of the AzureSqlDataProvider
        /// </summary>
        /// <param name="configuration">The configuration to use</param>
        /// <param name="tableSchemas">The provided data will be restricted to these schemas</param>
        public AzureSqlDataProvider(AzureSqlDataProviderConfiguration configuration, ReadOnlyCollection<string> tableSchemas)
            : this(configuration)
        {
            TableSchemas = tableSchemas;
        }

        protected AzureSqlDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            int version = info.GetInt32("AzureSqlDataProvider.Version");
            if (version >= 1)
            {
                Connection = new SqlConnection();
                Connection.ConnectionString = info.GetString("ConnectionString");
                SupportedElementTypes = (DbConnectionElementTypes)info.GetInt32("SupportedElementTypes");
                PrefixTableNameWithSchema = info.GetBoolean("PrefixTableNameWithSchema");
                TableSchemas = (ReadOnlyCollection<string>)info.GetValue("TableSchemas", typeof(ReadOnlyCollection<string>));
                SupportsAdvancedFiltering = info.GetBoolean("SupportsAdvancedFiltering");
            }
            InitSqlModifications();
        }

        [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("AzureSqlDataProvider.Version", 1);
            info.AddValue("ConnectionString", Connection.ConnectionString);
            info.AddValue("SupportedElementTypes", (int)SupportedElementTypes);
            info.AddValue("PrefixTableNameWithSchema", PrefixTableNameWithSchema);
            info.AddValue("TableSchemas", TableSchemas);
            info.AddValue("SupportsAdvancedFiltering", SupportsAdvancedFiltering);
        }
    }
}
