///////////////////////////////////////////////////////////
// Plugin PageRank : file PageRank.cs
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

    
    public class PageRankPlugin : CommandPlugin
    {

        private static string indexname, collection, column_id, column_links, column_score;


        public override Return OnExecute()
        {

            Sys.Log("Reading input values");

            indexname = Command.GetValue("index");
            collection = Command.GetValue("collection");
            column_id = Command.GetValue("column_id");
            column_links = Command.GetValue("column_links");
            column_score = Command.GetValue("column_score");

            if(Str.IsEmpty(indexname) || Str.IsEmpty(collection) || Str.IsEmpty(column_id) || Str.IsEmpty(column_links) || Str.IsEmpty(column_score))
            {
                Sys.Log("All PageRank parameters must be specified in Command options");
                Sys.Log("index: ", indexname);
                Sys.Log("collection: ", collection);
                Sys.Log("column_id: ", column_id);
                Sys.Log("column_links: ", column_links);
                Sys.Log("column_score: ", column_score);
                return Return.Error;
            }

            string alpha = Command.GetValue("alpha");
            string tolerance = Command.GetValue("tolerance");
            string checkSteps = Command.GetValue("checkSteps");

            double alpha_val = Str.IsEmpty(alpha) ? 0.85 : double.Parse(alpha);
            double tolerance_val = Str.IsEmpty(tolerance) ? 0.0001 : double.Parse(tolerance);
            int checkSteps_val = Str.IsEmpty(checkSteps) ? 10 : int.Parse(checkSteps);
            
            Dictionary<string, string> url2id = new Dictionary<string, string>();
            Dictionary<string, int> url2idx = new Dictionary<string, int>();
            List<string> pages = new List<string>();

            Sys.Log("Get pages");
            GetPages(pages, url2id, url2idx);

            int ndocs = pages.Count;
            //Sys.Log(ndocs," ", url2id.Count, " ", url2idx.Count);
            ArrayList matrix = new ArrayList(ndocs);
            for (int i = 0; i < ndocs; i++)
                matrix.Add(new List<int>());

            Sys.Log("Get Links");
            GetLinks(url2idx, matrix);

            Sys.Log("Start Page Rank");
            PageRank PR = new PageRank(matrix, alpha_val, tolerance_val / ndocs, checkSteps_val);
            double[] values = PR.ComputePageRank();

            Sys.Log("Results!");
            for (int i = 0; i < Math.Min(ndocs,50); i++) Sys.Log(pages[i], " ", values[i]);

            double min=double.MaxValue, max= double.MinValue;
            int imin=0, imax=0;
            for (int i = 0; i < ndocs; i++)
            {
                if(values[i] < min)
                {
                    min = values[i];
                    imin = i;
                }
                if (values[i] > max)
                {
                    max = values[i];
                    imax = i;
                }
            }
            Sys.Log("Minimum: ", pages[imin], " (", min, ")");
            Sys.Log("Maximum: ", pages[imax], " (", max, ")");


            Sys.Log("Update");
            if(min != max)
                UpdateRelevance(pages, url2id, url2idx, values, min, max);

            return Return.OK;
        }


        public void GetPages(List<string> pages, Dictionary<string, string> url2id, Dictionary<string, int> url2idx)
        {

            CCIndex index = CC.Current.Indexes[indexname];
            EngineClient client = null;

            try
            {

                client = EngineClientsPool.FromPool(index);

                String sqlquery = Str.And("select id,", column_id, " from ", indexname, " where collection = '", collection, "' and ", column_id, "<> ''");

                Sys.Log("processing query for ", sqlquery);

                Cursor cursor = client.ExecCursor(sqlquery);

                if (cursor != null)
                {

                    Sys.Log("Number of rows: ", cursor.CursorRowCount);

                    int duplicates = 0;

                    for (int i = 0; i < cursor.CursorRowCount; i++)
                    {
                        //Sys.Log("doc " + i);
                        string docid = cursor.GetColumn("id");
                        string pagerankid = cursor.GetColumn(column_id);

                        if (!url2id.ContainsKey(pagerankid))    // Duplicates id are possible...
                        {
                            pages.Add(pagerankid);
                            url2id[pagerankid] = docid;
                            url2idx[pagerankid] = i- duplicates;
                        }
                        else
                        {
                            duplicates++;
                        }

                        //Sys.Log("Added doc " + doc.Id);
                        cursor.MoveNext();
                    }

                }
                else
                    Sys.Log("No doc");

            }
            finally
            {
                EngineClientsPool.ToPool(client);
            }

        }

        public void GetLinks(Dictionary<string, int> url2idx, ArrayList matrix)
        {

            CCIndex index = CC.Current.Indexes[indexname];
            EngineClient client = null;

            try
            {

                client = EngineClientsPool.FromPool(index);

                String sqlquery = Str.And("select ", column_id, ",", column_links, " from ", indexname, " where collection = '", collection, "' and ", column_id, "<> ''");

                Sys.Log("processing query for ", sqlquery);

                Cursor cursor = client.ExecCursor(sqlquery);

                if (cursor != null)
                {

                    Sys.Log("Number of rows: ", cursor.CursorRowCount);

                    for (int i = 0; i < cursor.CursorRowCount; i++)
                    {
                        string pagerankid = cursor.GetColumn(column_id);
                        string links = cursor.GetColumn(column_links);
                        //Sys.Log("doc ", i, " ", pagerankid, " ", url2idx[pagerankid]);

                        List<int> doc = matrix[url2idx[pagerankid]] as List<int>;
                        if(links != null && Str.NEQ(links, ""))
                        {

                            foreach (string link in links.Split(';'))
                            {
                                if (url2idx.ContainsKey(link))
                                {
                                    doc.Add(url2idx[link]);
                                }
                            }

                        }

                        //Sys.Log("Added doc " + url2idx[pagerankid]);
                        cursor.MoveNext();
                    }

                }
                else
                    Sys.Log("No doc");

            }
            finally
            {
                EngineClientsPool.ToPool(client);
            }

        }

        private void UpdateRelevance(List<string> pages, Dictionary<string, string> url2id, Dictionary<string, int> url2idx, double[] values, double min, double max)
        {

            CCIndex index = CC.Current.Indexes[indexname];
            EngineClient client = null;

            try
            {

                client = EngineClientsPool.FromPool(index);

                foreach(string page in pages)
                {
                    int idx = url2idx[page];
                    string id = url2id[page];
                    double value = 0.50 * Math.Sqrt((values[idx] - min) / (max - min));  // Value will be between 0 and 50%

                    String sqlquery = String.Format("UPDATE {3} SET {2} = '{0}' WHERE id = '{1}';", value, id.Replace("'","''"), column_score, indexname);

                    //Sys.Log("processing query for ", sqlquery);

                    client.Exec(sqlquery);
                }

                
            }
            finally
            {
                EngineClientsPool.ToPool(client);
            }
        }
    }


    // Implementation of PageRank from https://github.com/jeffersonhwang/pagerank
    // License: https://github.com/jeffersonhwang/pagerank/blob/master/License.md
    // The original code was released by Vincent Kraeutler under a Creative Commons Attribution 2.5 License.

    public class PageRank
    {
        #region Private Fields

        ArrayList _incomingLinks, _leafNodes;
        Vector _numLinks;
        double _alpha, _convergence;
        int _checkSteps;

        #endregion

        #region Constructor

        public PageRank(ArrayList linkMatrix, double alpha = 0.85, double convergence = 0.0001, int checkSteps = 10)
        {
            Tuple<ArrayList, Vector, ArrayList> tuple = TransposeLinkMatrix(linkMatrix);
            _incomingLinks = tuple.Item1;
            _numLinks = tuple.Item2;
            _leafNodes = tuple.Item3;
            _alpha = alpha;
            _convergence = convergence;
            _checkSteps = checkSteps;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Convenience wrap for the link matrix transpose and the generator.
        /// See PageRankGenerator method for parameter descriptions
        /// </summary>
        public double[] ComputePageRank()
        {
            Vector final = null;
            foreach (Vector generator in PageRankGenerator(_incomingLinks, _numLinks, _leafNodes, _alpha, _convergence, _checkSteps))
                final = generator;

            return final.ToArray();
        }

        /// <summary>
        /// Transposes the link matrix which contains the links from each page. 
        /// Returns a Tuple of:  
        /// 1) pages pointing to a given page, 
        /// 2) how many links each page contains, and
        /// 3) which pages contain no links at all. 
        /// We want to know is which pages
        /// </summary>
        /// <param name="outGoingLinks">outGoingLinks[i] contains the indices of the pages pointed to by page i</param>
        /// <returns>A tuple of (incomingLinks, numOutGoingLinks, leafNodes)</returns>
        public Tuple<ArrayList, Vector, ArrayList> TransposeLinkMatrix(ArrayList outGoingLinks)
        {
            int nPages = outGoingLinks.Count;

            // incomingLinks[i] will contain the indices jj of the pages
            // linking to page i
            ArrayList incomingLinks = new ArrayList(nPages);
            for (int i = 0; i < nPages; i++)
                incomingLinks.Add(new List<int>());

            // the number of links in each page
            Vector numLinks = new Vector(nPages);

            // the indices of the leaf nodes
            ArrayList leafNodes = new ArrayList();
            for (int i = 0; i < nPages; i++)
            {
                List<int> values = outGoingLinks[i] as List<int>;
                if (values.Count == 0)
                    leafNodes.Add(i);
                else
                {
                    numLinks[i] = values.Count;
                    // transpose the link matrix
                    foreach (int j in values)
                    {
                        List<int> list = (List<int>)incomingLinks[j];
                        list.Add(i);
                        incomingLinks[j] = list;
                    }
                }
            }

            return new Tuple<ArrayList, Vector, ArrayList>(incomingLinks, numLinks, leafNodes);
        }

        /// <summary>
        /// Computes an approximate page rank vector of N pages to within some convergence factor.
        /// </summary>
        /// <param name="at">At a sparse square matrix with N rows. At[i] contains the indices of pages jj linking to i</param>
        /// <param name="leafNodes">contains the indices of pages without links</param>
        /// <param name="numLinks">iNumLinks[i] is the number of links going out from i.</param>
        /// <param name="alpha">a value between 0 and 1. Determines the relative importance of "stochastic" links.</param>
        /// <param name="convergence">a relative convergence criterion. Smaller means better, but more expensive.</param>
        /// <param name="checkSteps">check for convergence after so many steps</param>
        public IEnumerable<Vector> PageRankGenerator(ArrayList at, Vector numLinks, ArrayList leafNodes, double alpha, double convergence, int checkSteps)
        {
            int N = at.Count;
            int M = leafNodes.Count;

            Vector iNew = Ones(N) / N;
            Vector iOld = Ones(N) / N;

            bool done = false;

            int cpt = 0;

            while (!done)
            {
                // normalize every now and then for numerical stability
                iNew /= iNew.Sum();

                for (int i = 0; i < checkSteps; i++)
                {
                    // swap arrays
                    Vector temp = iOld;
                    iOld = iNew;
                    iNew = temp;

                    // an element in the 1 x I vector. 
                    // all elements are identical.
                    double oneIv = (1 - alpha) * iOld.Sum() / N;

                    // an element of the A x I vector.
                    // all elements are identical.
                    double oneAv = 0.0;
                    if (M > 0)
                        oneAv = alpha * Take(iOld, leafNodes).Sum() / N;

                    // the elements of the H x I multiplication
                    for (int j = 0; j < N; j++)
                    {
                        List<int> page = (List<int>)at[j];
                        double h = 0;

                        if (page.Count > 0)
                            h = alpha * Take(iOld, page).DotProduct(1.0 / Take(numLinks, page));

                        iNew[j] = h + oneAv + oneIv;
                    }
                }
                Vector diff = iNew - iOld;
                done = diff.SumMagnitudes() < convergence;

                Sys.Log("PAGE RANK: Iteration " + (cpt++) + " : error: " + diff.SumMagnitudes() + " / " + convergence);

                yield return iNew;
            }
        }

        public Vector Ones(int n)
        {
            Vector result = new Vector(n);
            for (int i = 0; i < result.Count(); i++)
                result[i] = 1.0;

            return result;
        }
        


        /// <summary>
        /// Simplified (numPy) take method: 1) axis is always 0, 2) first argument is always a vector
        /// </summary>
        /// <param name="vector1">List of values</param>
        /// <param name="vector2">List of indices</param>
        /// <returns>Vector containing elements from vector 1 at the indicies in vector 2</returns>
        private Vector Take(Vector vector1, IList vector2)
        {
            Vector result = new Vector(vector2.Count);
            for (int i = 0; i < vector2.Count; i++)
                result[i] = vector1[Convert.ToInt32(vector2[i])];

            return result;
        }

        #endregion
    }

    // Custom implementation of a Vector class
    // TODO: migrate to System.Numerics.Vector?

    public class Vector
    {
        private double[] data;

        public Vector(int size)
        {
            data = new double[size];
        }

        public double this[int index]
        {
            get
            {
                // get the item for that index.
                return data[index];
            }
            set
            {
                // set the item for this index. value will be of type Thing.
                data[index] = value;
            }
        }

        public static Vector operator /(Vector b, double c)
        {
            for (int i = 0; i < b.Count(); i++)
                b[i] /= c;
            return b;
        }

        public static Vector operator /(double b, Vector c)
        {
            for (int i = 0; i < c.Count(); i++)
                c[i] = b / c[i];
            return c;
        }

        public static Vector operator -(Vector b, Vector c)
        {
            Vector a = new Vector(b.Count());
            for (int i = 0; i < b.Count(); i++)
                a[i] = b[i] - c[i];
            return a;
        }

        public double Sum()
        {
            double sum = 0;
            for (int i = 0; i < Count(); i++)
            {
                sum += data[i];
            }
            return sum;
        }

        internal int Count()
        {
            return data.Length;
        }

        internal double DotProduct(Vector p)
        {
            double res = 0;
            for (int i = 0; i < Count(); i++)
                res += data[i] * p.data[i];
            return res;
        }

        internal double SumMagnitudes()
        {
            double sum = 0;
            for (int i = 0; i < Count(); i++)
            {
                sum += Math.Abs(data[i]);
            }
            return sum;
        }

        internal double[] ToArray()
        {
            return (double[])data.Clone();
        }
    }
}
