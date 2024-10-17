using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
// according to https://devblogs.microsoft.com/visualstudio/elevating-debugging-with-auto-decompilation-and-external-sources/ this is honored by VS even though obsolete
#pragma warning disable SYSLIB0025 // Type or member is obsolete
[assembly: SuppressIldasmAttribute()]
#pragma warning restore SYSLIB0025 // Type or member is obsolete
[assembly: AssemblyDescription("")]
[assembly: AssemblyConfiguration("")]
[assembly: AssemblyCompany("combit GmbH, www.combit.net")]
[assembly: AssemblyProduct("List & Label")]
[assembly: AssemblyCopyright("Copyright © combit GmbH")]
[assembly: AssemblyTrademark("combit and List & Label are registered trademarks of combit GmbH, Germany, www.combit.net")]
[assembly: AssemblyCulture("")]
#if NET_BUILD
    [assembly: System.Runtime.Versioning.SupportedOSPlatform("windows")]
#endif

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
[assembly: AssemblyVersion("30.0")]
[assembly: AssemblyFileVersion("30.0.0")]
[assembly: AssemblyInformationalVersion("30.0.0.0")]