///////////////////////////////////////////////////////////
// Plugin common.function.Base64 : file Base64Decode.cs
//
using Sinequa.Common;
using Sinequa.Plugins;

using System;
using System.Text;

namespace Sinequa.Plugin
{
    /// <summary>
    /// Function plug-in computing a string from the Base64 encoded first parameter.
    /// </summary>
    /// <remarks>
    /// Author: konrad.holl@accenture.com
    /// </remarks>
    public class Base64Decode : FunctionPlugin
    {
        /// <summary>
        /// Method that returns the value of the function
        /// </summary>
        /// <param name="ctxt">Sinequa.Common.IDocContext object giving access to the function call context</param>
        /// <param name="values">Array of the arguments passed when calling the function</param>
        /// <returns>A character string corresponding to the evaluation of the function</returns>
        /// <remarks>https://doc.sinequa.com/en.sinequa-es.v11/Content/en.sinequa-es.devDoc.plugin.function-plugin.html</remarks>
        public override string GetValue(IDocContext ctxt, params string[] args)
        {
            return (args.Length != 1) ? String.Empty : Encoding.Default.GetString(Convert.FromBase64String(args[0]));
        }
    }
}
