# Project Description
To connect to Cassandra, Redis, Firebird, NuoDB and PostgreSQL data, use the DataProvider classes provided in the corresponding assemblies. Please note that they require additional files from the respective projects:

- Provider for Cassandra (combit.ListLabel27.CassandraDataProvider.dll): uses NuGet package
- Provider for Firebird (combit.ListLabel27.FirebirdConnectionDataProvider.dll): uses NuGet package
- Provider for NuoDB (combit.ListLabel27.NuoDbConnectionDataProvider.dll): uses NuGet package
- Provider for Npgsql PostgreSQL (combit.ListLabel27.NpgsqlConnectionDataProvider.dll): uses NuGet package
- Provider for Redis (combit.ListLabel27.RedisDataProvider.dll): uses NuGet package

You may need to adapt the reference path for combit.ListLabel27.dll. Also, make sure to update/reinstall all required NuGet packages.

# Further Information
To connect to DB2, MySQL and Oracle data, use the DataProvider classes that are integrated into the combit.ListLabel27 Assembly. They use the usual DbProviderFactory mechanism to detect the installed drivers.

# Disclaimer
Please note that combit is not responsible for the content of these websites.

The providers can be used on an "as is" basis and are not officially supported. Issues can still be reported via our website. Of course these issues will be investigated, but providing fixes or enhancements cannot be guaranteed.