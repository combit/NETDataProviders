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
    /// Provides the configuration data for the Azure SQL database
    /// </summary>
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
            if (!String.IsNullOrEmpty(ConnectionStringOverride)) 
            { 
                return  ConnectionStringOverride; 
            }

            if (AuthenticationType == AzureSqlDataProviderAuthenticationType.AzureADIdentity && (String.IsNullOrEmpty(UserName) || String.IsNullOrEmpty(Password)))
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
    /// A data provider that allows to connect to Azure SQL databases. All you need is a <see cref="AzureSqlDataProviderConfiguration"/>.
    /// </summary>
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
            if (!String.IsNullOrEmpty(tableSchema))
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
