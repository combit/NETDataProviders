using System;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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
[assembly: AssemblyFileVersion("29.1.0.0")]

#if NET_BUILD
    [assembly: AssemblyVersion("29.1")]
    [assembly: System.Runtime.Versioning.SupportedOSPlatform("windows")]
#else // .NET Framework 4
    [assembly: AssemblyVersion("29.1.*")]
#endif