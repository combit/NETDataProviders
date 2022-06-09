# List & Label .NET Data Providers
This repository contains the sources to connect to Cassandra, Firebird, NuoDB, PostgreSQL, Redis and Schema-Aware Json data with the **List & Label Reporting Tool**. Please note that they require additional files from their respective projects via NuGet.

You may need to adapt the reference path for combit.ListLabel??.dll. Also, make sure to update/reinstall all required NuGet packages.

To connect to DB2, MySQL and Oracle data, use the DataProvider classes that are integrated into the combit.ListLabel?? assembly. They use the usual DbProviderFactory mechanism to detect the installed drivers.

# Disclaimer
combit is not responsible for the content of the above-mentioned NuGet packages.

The providers can be used on an "as is" basis and are not officially supported. Issues can still be reported. Of course these issues will be investigated, but providing fixes or enhancements cannot be guaranteed.

# Contributions
We're happy to receive pull requests for any improvements on this repository.

# About List & Label and Where to Get
List & Label is our **Reporting Tool for Software Developers** for desktop, web and cloud applications. For further information and a fully functional free 30-day trial version please visit our [website](https://www.combit.com/reporting-tool/).

# Contact
Please contact us at [github@combit.com](mailto:github@combit.com) with any additional feedback.