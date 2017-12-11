# Project description
The DataProvider classes provided in the following assemblies can be used to connect to Firebird, NuoDb and PostgeSQL data:
combit.ListLabel23.FirebirdConnectionDataProvider.dll
combit.ListLabel23.NpgsqlConnectionDataProvider.dll
combit.ListLabel23.NuoDbConnectionDataProvider.dll

They require additional files from the respective projects
- Provider for Firebird, see http://www.firebirdsql.org/en/net-provider/, compiled against version 4.6.2.0
- Provider for Npgsql PostgreSQL connection, NuGet package "npgsql", compiled against version 3.0.3 for .NET 4.5
- Provider for NuoDB, see http://www.nuodb.com/devcenter, compiled against NuoDb.Data.Client version 1.1.0.4

# Further information
The DataProvider classes to connect to DB2, MySql and Oracle are integrated into the combit.ListLabel23 Assembly and are only provided for reference here, they require the following .NET ADO drivers:
- Provider for DB2, see http://www-01.ibm.com/support/docview.wss?uid=swg21385217 (search for IBM Data Server Client (Windows/x86-32 32 bit)
- Provider for MySQL, see https://dev.mysql.com/downloads/connector/net/6.9.html

The Oracle DataProvider supports the two ADO.NET drivers that are part of ODAC/ODP.NET. If installed, List & Label uses the new "Oracle.ManagedDataAccess.Client" driver. We recommend to use this driver, while providing an automatic fallback to the legacy "Oracle.DataAccess.Client" driver. Both drivers require a separate installation.
- 32bit ODAC, see http://www.oracle.com/technetwork/database/windows/downloads/utilsoft-087491.html
- 64bit ODAC, see http://www.oracle.com/technetwork/database/windows/downloads/index-090165.html

# Disclaimer
Please note that combit is not responsible for the content of these websites.

The providers can be used on an "as is" basis and are not officially supported. Issues can still be reported via our website. Of course these issues will be investigated, but providing fixes or enhancements cannot be guaranteed.
