///////////////////////////////////////////////////////////
// Plugin GeoSearch : file GeoFunctions.cs
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
using System.Globalization;
using System.Text.RegularExpressions;
//using Sinequa.Ml;

namespace Sinequa.Plugin
{
    // Create a POINT primitive from a latitude and a longitude (in decimal degrees, eg. "45.2548962" and "2.354874612")
    public class GeoPoint : FunctionPlugin
    {

        public override string GetValue(IDocContext ctxt, params string[] values)
        {
            string latitude = values[0];
            string longitude = values[1];
            return "POINT(" + longitude + " " + latitude + ")";
        }

    }

    // Create a MULTIPOINT primitive from a list of coordinates (in decimal degrees, eg. "45.2548962" and "2.354874612").
    // The coordinates are given as a list of semicolon-separated latitudes (first input) and longitudes (second input)
    public class GeoMultipoint : FunctionPlugin
    {
        public override string GetValue(IDocContext ctxt, params string[] values)
        {
            if (Str.IsEmpty(values[0]) || Str.IsEmpty(values[1]))
                return "";

            string[] lats = values[0].Split(';');
            string[] lngs = values[1].Split(';');

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < lats.Length; i++)
            {
                if (sb.Length > 0)
                {
                    sb.Append(",");
                }
                sb.Append("(");
                sb.Append(lngs[i]);
                sb.Append(" ");
                sb.Append(lats[i]);
                sb.Append(")");
            }

            return "MULTIPOINT(" + sb.ToString() + ")";
        }
    }

    // Converts coordinates from the Sexagesimal format (eg. 48°51′24″N) to the regular GPS system (decimal degrees, eg. "45.2548962" and "2.354874612")
    // See https://en.wikipedia.org/wiki/Geographic_coordinate_conversion
    public class Sexagesimal2GPS : FunctionPlugin {

        public override string GetValue(IDocContext ctxt, params string[] values)
        {
            double value = 0;
            
            string pattern = @"^\s*(\d+)\s*°\s*(\d+)?\s*['′]?\s*([\d.]+)?\s*[″""]?\s*([NSEOW])\s*$";

            Match match = Regex.Match(values[0], pattern);

            if(match.Success) {
                double deg = double.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
                double min = match.Groups[2].Value == ""? 0 : double.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture);
                double sec = match.Groups[3].Value == ""? 0 : double.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture);
                string direction = match.Groups[4].Value;
                double sign = direction == "N" || direction == "E" ? +1 : -1;

                value = sign * (deg + min / 60.0 + sec / 3600);
            }
            else {
                throw new Exception("Incorrect format: "+values[0]);
            }

            return value.ToString(CultureInfo.InvariantCulture);
        }
    }

    // Converts coordinates from the Lambert 2 Extended system to the regular GPS system (decimal degrees, eg. "45.2548962" and "2.354874612")
    // See https://fr.wikipedia.org/wiki/Projection_conique_conforme_de_Lambert#Lambert_carto_et_Lambert_%C3%A9tendu
    // Translated to C# from https://gist.github.com/lovasoa/096d8be82520ea6b0abe
    public class LamberExt2GPS : FunctionPlugin
    {

        public override string GetValue(IDocContext ctxt, params string[] values)
        {

            double x = double.Parse(values[0].Trim(), CultureInfo.InvariantCulture);
            double y = double.Parse(values[1].Trim(), CultureInfo.InvariantCulture);
            bool is_x = Str.EQNC(values[2].Trim(), "x");

            double n = 0.7289686274;
            double c = 11745793.39;            // En mètres
            double Xs = 600000.0;          // En mètres
            double Ys = 8199695.768;          // En mètres
            double l0 = 0.0;                    //correspond à la longitude en radian de Paris (2°20'14.025" E) par rapport à Greenwich
            double e = 0.08248325676;           //e du NTF (on le change après pour passer en WGS)
            double eps = 0.00001;     // précision


            /*
			* Conversion Lambert 2 -> NTF géographique : ALG0004
			*/
            double R = Math.Sqrt(((x - Xs) * (x - Xs)) + ((y - Ys) * (y - Ys)));
            double g = Math.Atan((x - Xs) / (Ys - y));
            double l = l0 + (g / n);
            double L = -(1 / n) * Math.Log(Math.Abs(R / c));
            double phi0 = 2 * Math.Atan(Math.Exp(L)) - (Math.PI / 2.0);
            double phiprec = phi0;
            double phii = 2 * Math.Atan((Math.Pow(((1 + e * Math.Sin(phiprec)) / (1 - e * Math.Sin(phiprec))), e / 2.0) * Math.Exp(L))) - (Math.PI / 2.0);
            while (!(Math.Abs(phii - phiprec) < eps))
            {
                phiprec = phii;
                phii = 2 * Math.Atan((Math.Pow(((1 + e * Math.Sin(phiprec)) / (1 - e * Math.Sin(phiprec))), e / 2.0) * Math.Exp(L))) - (Math.PI / 2.0);
            }
            double phi = phii;
            /*
			* Conversion NTF géogra$phique -> NTF cartésien : ALG0009
			*/
            double a = 6378249.2;
            double h = 100;         // En mètres
            double N = a / (Math.Pow((1 - (e * e) * (Math.Sin(phi) * Math.Sin(phi))), 0.5));
            double X_cart = (N + h) * Math.Cos(phi) * Math.Cos(l);
            double Y_cart = (N + h) * Math.Cos(phi) * Math.Sin(l);
            double Z_cart = ((N * (1 - (e * e))) + h) * Math.Sin(phi);
            /*
			* Conversion NTF cartésien -> WGS84 cartésien : ALG0013
			*/
            // Il s'agit d'une simple translation
            double XWGS84 = X_cart - 168;
            double YWGS84 = Y_cart - 60;
            double ZWGS84 = Z_cart + 320;
            /*
			* Conversion WGS84 cartésien -> WGS84 géogra$phique : ALG0012
			*/

            double l840 = 0.04079234433;    // 0.04079234433 pour passer dans un référentiel par rapport au méridien
                                            // de Greenwich, sinon mettre 0

            e = 0.08181919106;              // On change $e pour le mettre dans le système WGS84 au lieu de NTF
            a = 6378137.0;
            double P = Math.Sqrt((XWGS84 * XWGS84) + (YWGS84 * YWGS84));
            double l84 = l840 + Math.Atan(YWGS84 / XWGS84);
            double phi840 = Math.Atan(ZWGS84 / (P * (1 - ((a * e * e))
                                        / Math.Sqrt((XWGS84 * XWGS84) + (YWGS84 * YWGS84) + (ZWGS84 * ZWGS84)))));
            double phi84prec = phi840;
            double phi84i = Math.Atan((ZWGS84 / P) / (1 - ((a * e * e * Math.Cos(phi84prec))
                    / (P * Math.Sqrt(1 - e * e * (Math.Sin(phi84prec) * Math.Sin(phi84prec)))))));
            while (!(Math.Abs(phi84i - phi84prec) < eps))
            {
                phi84prec = phi84i;
                phi84i = Math.Atan((ZWGS84 / P) / (1 - ((a * e * e * Math.Cos(phi84prec))
                        / (P * Math.Sqrt(1 - ((e * e) * (Math.Sin(phi84prec) * Math.Sin(phi84prec))))))));
            }
            double phi84 = phi84i;


            return is_x ? (phi84 * 180.0 / Math.PI).ToString(CultureInfo.InvariantCulture) : (l84 * 180.0 / Math.PI).ToString(CultureInfo.InvariantCulture);

        }

    }
}
