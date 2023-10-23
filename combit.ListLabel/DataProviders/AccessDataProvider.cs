using System;
using System.Data;
using System.Data.OleDb;
using System.Net;
using System.Runtime.Serialization;
using System.Security;

namespace combit.Reporting.DataProviders
{
    [Serializable]
    public sealed class AccessDataProvider : OleDbConnectionDataProvider
    {
        public string FileName { get; private set; }

        public AccessDataProvider(string fileName)
            : this(fileName, String.Empty) { }

        public AccessDataProvider(string fileName, string password)
            : base(GetOleDbConnection(fileName, password))
        {
            FileName = fileName;
            SupportsAdvancedFiltering = true;
            base.UseMsAccessSqlSyntax = true;
        }

        private AccessDataProvider(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            FileName = info.GetString("AccessFilePath");
            base.UseMsAccessSqlSyntax = true;
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            base.GetObjectData(info, context);
            info.AddValue("AccessFilePath", FileName);
        }

        private static OleDbConnection GetOleDbConnection(string path, string dbPassword)
        {
            //Is ACE installed?
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
            if (aceFound) //use ACE 12
            {
                connectionString = String.Format(connectionString, path, dbPassword);
            }
            else if (!aceFound && IntPtr.Size == 4) //use Jet 4.0
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