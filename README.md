# Project Description
To connect to Cassandra, Redis, Firebird, NuoDB and PostgreSQL data, use the DataProvider classes provided in the corresponding assemblies. Please note that they require additional files from the respective projects:

- Provider for Cassandra (combit.ListLabel24.CassandraDataProvider.dll): uses NuGet package
- Provider for Firebird (combit.ListLabel24.FirebirdConnectionDataProvider.dll): uses NuGet package
- Provider for NuoDB (combit.ListLabel24.NuoDbConnectionDataProvider.dll): see http://www.nuodb.com/devcenter, tested with NuoDb.Data.Client version 2.3.0.9
- Provider for Npgsql PostgreSQL (combit.ListLabel24.NpgsqlConnectionDataProvider.dll): uses NuGet package
- Provider for Redis (combit.ListLabel24.RedisDataProvider.dll): uses NuGet package

You may need to adapt the reference path for combit.ListLabel24.dll. Also, make sure to update/reinstall all required NuGet packages.

# Further Information
To connect to DB2, MySQL and Oracle data, use the DataProvider classes that are integrated into the combit.ListLabel24 Assembly. They use the usual DbProviderFactory mechanism to detect the installed drivers.

# Disclaimer
Please note that combit is not responsible for the content of these websites.

The providers can be used on an "as is" basis and are not officially supported. Issues can still be reported via our website. Of course these issues will be investigated, but providing fixes or enhancements cannot be guaranteed.