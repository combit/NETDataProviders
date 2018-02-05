# Project Description
To connect to Firebird, NuoDb and PostgreSQL data, use the DataProvider classes provided in the corresponding assemblies. Please note that they require additional files from the respective projects:
- Provider for Firebird (combit.ListLabel23.FirebirdConnectionDataProvider.dll): uses NuGet package
- Provider for NuoDB (combit.ListLabel23.NuoDbConnectionDataProvider.dll): see http://www.nuodb.com/devcenter, compiled against NuoDb.Data.Client version 1.1.0.4
- Provider for Npgsql PostgreSQL (combit.ListLabel23.NpgsqlConnectionDataProvider.dll): uses NuGet package

# Further Information
To connect to DB2, MySQL and Oracle data, use the DataProvider classes that are integrated into the combit.ListLabel23 Assembly. They are only provided for reference here and require the following .NET ADO drivers:
- Provider for DB2: see http://www-01.ibm.com/support/docview.wss?uid=swg21385217 (search for IBM Data Server Client (Windows/x86-32 32 bit)
- Provider for MySQL: see https://dev.mysql.com/downloads/connector/net/6.9.html

The Oracle DataProvider supports the two ADO.NET drivers that are part of ODAC/ODP.NET. If installed, List & Label uses the new "Oracle.ManagedDataAccess.Client" driver. We recommend to use this driver, while providing an automatic fallback to the legacy "Oracle.DataAccess.Client" driver. Both drivers require a separate installation.
- 32bit ODAC, see http://www.oracle.com/technetwork/database/windows/downloads/utilsoft-087491.html
- 64bit ODAC, see http://www.oracle.com/technetwork/database/windows/downloads/index-090165.html

# Disclaimer
Please note that combit is not responsible for the content of these websites.

The providers can be used on an "as is" basis and are not officially supported. Issues can still be reported via our website. Of course these issues will be investigated, but providing fixes or enhancements cannot be guaranteed.
