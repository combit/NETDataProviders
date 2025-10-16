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
#if NET_BUILD && !LLCP
    [assembly: System.Runtime.Versioning.SupportedOSPlatform("windows")]
#endif

#if LLCP
#if NET8_0
    [assembly: AssemblyInformationalVersion("31.1.0.0 (.NET 8)")]
#elif NET9_0
    [assembly: AssemblyInformationalVersion("31.1.0.0 (.NET 9)")]
#else
    [assembly: AssemblyInformationalVersion("31.1.0.0")]
#endif
#else
    [assembly: AssemblyInformationalVersion("31.1.0.0")]
#endif


// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
[assembly: AssemblyVersion("31.1")]
[assembly: AssemblyFileVersion("31.1.0")]
