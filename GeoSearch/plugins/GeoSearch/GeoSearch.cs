///////////////////////////////////////////////////////////
// Plugin GeoSearch : file GeoSearch.cs
//

using System;
using System.Collections.Generic;
using System.Collections;
using System.Text;
using Sinequa.Common;
using Sinequa.Configuration;
using Sinequa.Plugins;
using Sinequa.Connectors;
using Sinequa.Indexer;
using Sinequa.Search;
using Sinequa.Engine.Client;
//using Sinequa.Ml;

namespace Sinequa.Plugin
{

    public class GeoSearch : QueryPlugin
    {

        public static string GEO_PLUGIN_NAME = "mygeoplugin";
        public static string GEO_INDEX_NAME = "geo2d";
        public static string FILTER_MODE = "intersection";
        public static string REQUEST_PARAM = "geoselect";

        public override void AddAdditionalWhereClause(StringBuilder sb)
        {
            base.AddAdditionalWhereClause(sb);

            string geoselect = Request.GetValue(REQUEST_PARAM);
            if (geoselect != null)
            {
                //Sys.Log(geoselect);
                string[] coords = geoselect.Split(';');
                if (coords.Length != 4)
                {
                    Sys.Log("ERROR: Geo selection must be provided as 4 semicolon-separated numbers (latitude NW;longitude NW;latitude SE;longitude SE)");
                    return;
                }
                string polygon = Str.Add(new StringBuilder(), "[", coords[2], ",", coords[0], "],[", coords[2], ",", coords[1], "],[", coords[3], ",", coords[1], "],[", coords[3], ",", coords[0], "],[", coords[2], ",", coords[0], "]").ToString();
                Str.Add(sb, And(), "JSON('{\"type\": \"index-plugin-predicate\", \"target\": \"", GEO_PLUGIN_NAME, "\", \"params\": {\"op\":\"", FILTER_MODE, "\", \"a\":{ \"type\" : \"index\", \"name\" : \"", GEO_INDEX_NAME, "\" }, \"b\":{ \"type\":\"Feature\", \"properties\":{}, \"geometry\":{\"type\":\"Polygon\",\"coordinates\":[[", polygon, "]]}} } }')");
            }
        }

    }

}
