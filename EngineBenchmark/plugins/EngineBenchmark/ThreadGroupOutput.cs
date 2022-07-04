using Sinequa.Common;
using Sinequa.Common.Collections;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Xml.Linq;

namespace Sinequa.Plugin
{

	public class ThreadGroupOutput
	{
		private ThreadGroup _threadGroup;
		private string _commandHost;
		private readonly object syncLock = new object();

		//thread group iteration
		public int iteration { get; private set; }
		//info
		public DateTime dStart { get; private set; }
		public DateTime dEnd { get; private set; }
		public string sql { get; private set; }
		public int threadId { get; private set; }
		public bool querySuccess { get; private set; }
		public bool parsingError { get; private set; }
		public bool dumpError { get; private set; }
		public Dictionary<string, string> dParams { get; private set; }
		public string engineClientName { get; private set; }
		public double cursorSize { get; private set; }
		public double cursorSizeMB
		{
			get { return cursorSize / 1000000; }
		}

		//client timers
		public double clientFromPool { get; private set; }
		public double clientToPool { get; private set; }

		//query timers
		public double processingTime { get; private set; }
		public double cacheHit { get; private set; }
		public double rowFetchTime { get; private set; }
		public double matchingRowCount { get; private set; }
		public double postGroupByMatchingRowCount { get; private set; }
		public double totalQueryTime { get; private set; }
		public double readCursor { get; private set; }

		//Curosr Network And Deserialization
		public double curosrNetworkAndDeserialization { get; private set; }

		//search RWA
		public List<(string engineName, string index, double duration)> IQLSearchRWA { get; private set; } = new List<(string engineName, string index, double duration)>();
		//FullText Search RWA
		public List<(string engineName, string index, double duration)> IQLFullTextSearchRWA { get; private set; } = new List<(string engineName, string index, double duration)>();
		//Execute DBQuery
		public List<(string engineName, string index, double duration)> IQLExecuteDBQuery { get; private set; } = new List<(string engineName, string index, double duration)>();
		//Fetching DBQuery
		public List<(string engineName, string index, double duration)> IQLFetchingDBQuery { get; private set; } = new List<(string engineName, string index, double duration)>();
		//AcqRLk
		public List<(string engineName, string index, double duration)> IQLAcqRLk { get; private set; } = new List<(string engineName, string index, double duration)>();

		//AcqMRdLk duration
		public double IQLAcqMRdLk { get; private set; }
		//AcqDBRdLk
		public double IQLAcqDBRdLk { get; private set; }
		//NetworkNotificationToWorkerStart
		public double IQLNetworkNotificationToWorkerStart { get; private set; }
		//MsgDeserialize
		public double IQLMsgDeserialize { get; private set; }
		//query processor
		public double IQLQueryProcessorParse { get; private set; }
		//MergeAttributes (brokering)
		public double IQLMergeAttributes { get; private set; }
		//broker engine
		public string IQLBrokerEngine { get; private set; }
		//broker clients
		public List<string> IQLBorkerClients { get; private set; } = new List<string>();
		//thread count (header)
		public int IQLHeaderThreadCount { get; private set; }
		//thread count (engines)
		public Dictionary<string, int> dIQLEngineThreadCount { get; private set; } = new Dictionary<string, int>();
		//Distributions
		public List<(string engineName, string distribution, double duration)> IQLDistribution { get; private set; } = new List<(string engineName, string distCorl, double duration)>();
		//Correlations
		public List<(string engineName, string correlation, double duration)> IQLCorrelation { get; private set; } = new List<(string engineName, string distCorl, double duration)>();

		//RFMBoost
		//RFM:exact
		public double IQLRFMBoostExact { get; private set; }
		//RFM:similar
		public double IQLRFMBoostSimilar { get; private set; }

		//Cursor Size Breakdown
		public Dictionary<string, long> dCursorSizeBreakdown { get; private set; } = new Dictionary<string, long>();

		public ThreadGroupOutput(ThreadGroup threadGroup, int id, int threadId, DateTime start, DateTime end)
		{
			this._threadGroup = threadGroup;
			this._commandHost = this._threadGroup.engineBenchmark.Command.Identity.Node.Host;
			this.iteration = id;
			this.threadId = threadId;
			this.dStart = start;
			this.dEnd = end;
			this.querySuccess = false;
			this.parsingError = false;
			this.dumpError = false;
		}

		public bool SetSuccess(BenchmarkQuery query)
		{
			this.querySuccess = query.success;
			return this.querySuccess;
		}

		public void SetParsingError()
        {
			this.parsingError = true;
        }

		public void SetDumpError()
		{
			this.dumpError = true;
		}

		public bool SetSQL(BenchmarkQuery query)
		{
			if (String.IsNullOrEmpty(query.sql)) return false;
			this.sql = query.sql;
			return true;
		}

		public bool SetInfo(BenchmarkQuery query, Dictionary<string, string> dParams)
		{
			if (String.IsNullOrEmpty(query.engineName) || dParams == null) return false;
			this.engineClientName = query.engineName;
			this.dParams = dParams;
			this.cursorSize = query.cursorSize;
			return true;
		}

		public bool SetClientTimers(BenchmarkQuery query)
		{
			this.clientFromPool = query.clientFromPoolTimer;
			this.clientToPool = query.clientToPoolTimer;
			return true;
		}

		public bool SetQueryTimers(BenchmarkQuery query)
		{
			this.processingTime = query.processingTime;
			this.cacheHit = query.cacheHit;
			this.rowFetchTime = query.rowFetchTime;
			this.matchingRowCount = query.matchingRowCount;
			this.postGroupByMatchingRowCount = query.postGroupByMatchingRowCount;
			this.totalQueryTime = query.totalQueryTimer;
			this.readCursor = query.readCursorTimer;
			return true;
		}

		public bool SetCursorNetworkAndDeserialization(BenchmarkQuery query)
		{
			this.curosrNetworkAndDeserialization = query.cursorNetworkAndDeserializationTimer;
			return true;
		}

		public bool SetInternalQueryLog(BenchmarkQuery query)
		{
			Stopwatch sw = new Stopwatch();
			sw.Start();

			if (String.IsNullOrEmpty(query.internalQueryLog)) return false;

			XDocument xInternalQueryLog = XDocument.Parse(query.internalQueryLog);
			if (xInternalQueryLog == null)
			{
				Sys.LogError($"Cannot parse InternalQueryLog [{query.internalQueryLog}]");
				return false;
			}

			//AcqMRdLk duration & start - AcqDBRdLk - NetworkNotificationToWorkerStart - MsgDeserialize - QueryProcessorParse
			if (_threadGroup.conf.outputIQLHeader) GetFromIQL_Header_Duration(xInternalQueryLog);
			if (_threadGroup.conf.outputIQLThreadCount) GetFromIQL_Header_Tid_Distinct(xInternalQueryLog);

			//RFMBoost
			if (_threadGroup.conf.outputRFMBoost)
            {
				if(xInternalQueryLog.Descendants("RFMBoost").Count() > 0) GetFromIQL_RFMBoost(xInternalQueryLog.Descendants("RFMBoost").First());
			}
				

			//no brokering, no engine tag
			if (xInternalQueryLog.Descendants("Engine").Count() == 0)
			{
				string engineName = engineClientName;

				//indexes list
				//<IndexSearch index="myIndex">
				List<XElement> lIndexElem = xInternalQueryLog.Descendants("IndexSearch").ToList();

				//SearchRWA & DBQuery
				if (_threadGroup.conf.outputIQLSearchRWA || _threadGroup.conf.outputIQLDBQuery || _threadGroup.conf.outputIQLAcqRLk) GetFromIQL_SearchRWA_DBQuery_AcqRLk_Duration(lIndexElem, engineName);

				//distribution & correlations
				if (_threadGroup.conf.outputIQLDistributionsCorrelations) GetFromIQL_Distribution_Correlation_Duration(xInternalQueryLog.Root, engineName);

				//distinct thread count
				if (_threadGroup.conf.outputIQLThreadCount) GetFromIQL_Engine_Tid_Distinct(xInternalQueryLog.Root, engineName);
			}
			//brokering, one or multiple engine tags
			else
			{
				//loop on <Engine> nodes
				List<XElement> lEngineElems = xInternalQueryLog.Descendants("Engine").ToList();
				foreach (XElement engineElem in lEngineElems)
				{
					//<Engine name="srpc://HOST:PORT">
					string engineName = EngineSRPCToEngineName(engineElem.Attribute(XName.Get("name")).Value);

					//indexes list
					//<IndexSearch index="myIndex">
					List<XElement> lIndexElem = engineElem.Descendants("IndexSearch").ToList();

					//SearchRWA - DBQuery - AcqRLk
					if (_threadGroup.conf.outputIQLSearchRWA || _threadGroup.conf.outputIQLDBQuery || _threadGroup.conf.outputIQLAcqRLk) GetFromIQL_SearchRWA_DBQuery_AcqRLk_Duration(lIndexElem, engineName);

					//distribution & correlations
					if (_threadGroup.conf.outputIQLDistributionsCorrelations) GetFromIQL_Distribution_Correlation_Duration(engineElem, engineName);

					//distinct thread count
					if (_threadGroup.conf.outputIQLThreadCount) GetFromIQL_Engine_Tid_Distinct(engineElem, engineName);

					//brokering
					if (_threadGroup.conf.outputIQLBrokering)
					{
						//broker client(s) - engines where SQL query has been propagated
						IQLBorkerClients.AddUnique(engineName);
					}
				}

				//brokering
				if (_threadGroup.conf.outputIQLBrokering)
				{
					//broker is always engine from EngineClient
					IQLBrokerEngine = engineClientName;

					//MergeAttributes
					GetFromIQL_MergeAttributes_Duration(xInternalQueryLog);
				}
			}

			sw.Stop();
			Sys.Log2(200, $"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] Timer - SetInternalQueryLog [{Sys.TimerGetText(sw.ElapsedMilliseconds)}]");

			return true;
		}

		private bool GetTimingDuration(XElement xElem, string attrName, out double timing)
        {
			timing = -1;
			if (xElem == null) return false;

			XElement timingElem = xElem.Descendants("timing").SingleOrDefault(x => Str.EQNC(x.Attribute("name").Value, attrName));
			if (timingElem == null)
			{
				Sys.LogWarning($"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] Cannot find timing [{attrName}] in [{xElem}]");
				return false;
			}

			return GetTimingDuration(timingElem, out timing);
		}

		private bool GetTimingDuration(XElement xElem, out double timing)
        {
			timing = -1;
			if (xElem == null) return false;

			string attrName = xElem.Attribute("name").Value;

			string strTiming = Str.ParseToSep(xElem.Attribute("duration").Value, ' ');
			if (String.IsNullOrEmpty(strTiming))
			{
				Sys.LogWarning($"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] Cannot find timing duration [{attrName}] in [{xElem}]");
				return false;
			}

			if (!double.TryParse(strTiming, out timing))
			{
				Sys.LogWarning($"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] Cannot parse timing duration [{attrName}] [{strTiming}]");
				return false;
			}

			Sys.Log2(200, $"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] GetTimingDuration [{attrName}] [{timing}]");


			return true;
        }

		private bool GetFromIQL_Header_Duration(XDocument xInternalQueryLog)
        {
			if (xInternalQueryLog == null) return false;

			double d;

			//<timing name='AcqMRdLk' duration='0.00 ms' start='2.68 ms' tid='17'/>
			if (GetTimingDuration(xInternalQueryLog.Root, "AcqMRdLk", out d)) IQLAcqMRdLk = d;
			else return false;
			//<timing name='AcqDBRdLk' duration='0.00 ms' start='2.68 ms' tid='17'/>
			if (GetTimingDuration(xInternalQueryLog.Root, "AcqDBRdLk", out d)) IQLAcqDBRdLk = d;
			else return false;
			//<timing name='NetworkNotificationToWorkerStart' duration='0.21 ms' start='0.21 ms' tid='17'/>
			if (GetTimingDuration(xInternalQueryLog.Root, "NetworkNotificationToWorkerStart", out d)) IQLNetworkNotificationToWorkerStart = d;
			else return false;
			//<timing name='MsgDeserialize' duration='0.02 ms' start='0.23 ms' tid='17'/>
			if (GetTimingDuration(xInternalQueryLog.Root, "MsgDeserialize", out d)) IQLMsgDeserialize = d;
			else return false;
			//<timing name='QueryProcessor::Parse' duration='2.32 ms' start='0.33 ms' tid='17'/>
			if (GetTimingDuration(xInternalQueryLog.Root, "QueryProcessor::Parse", out d)) IQLQueryProcessorParse = d;
			else return false;
			return true;
		}

		private bool GetFromIQL_Header_Tid_Distinct(XDocument xInternalQueryLog)
		{
			if (xInternalQueryLog == null) return false;

			string[] nodesTimingToMatch = { "AcqMRdLk", "AcqDBRdLk", "NetworkNotificationToWorkerStart", "MsgDeserialize", "QueryProcessor::Parse" };

			List<XElement> l = xInternalQueryLog.Descendants("timing").Where(x => Str.EQNCN(x.Attribute("name").Value, nodesTimingToMatch) && x.Attribute("tid") != null).ToList();

            if (l == null || l.Count != nodesTimingToMatch.Length)
            {
				Sys.LogWarning($"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] Cannot find {String.Join(",", nodesTimingToMatch)} elements in XML [{xInternalQueryLog}]");
				return false;
            }

			l = l.Where(x => x.Attribute("tid") != null).ToList();

			//in some cases not TID
			if (l == null || l.Count != nodesTimingToMatch.Length) IQLHeaderThreadCount = 0;
			else IQLHeaderThreadCount = l.Select(x => x.Attribute("tid").Value).Distinct().Count();

			Sys.Log2(200, $"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] GetFromIQL_Header_Tid_Distinct [{IQLHeaderThreadCount}]");

			return true;
		}

		//SearchRWA - FullTextSearchRWA - ExecuteDBQuery - Fetching DBQuery - AcqRLk
		private bool GetFromIQL_SearchRWA_DBQuery_AcqRLk_Duration(List<XElement> lIndexElem, string engineName)
        {
			if (lIndexElem == null || lIndexElem.Count == 0) return false;

			//add to static dictionary of <engine><list<indexes>>
			//<IndexSearch index="myIndex">
			List<string> lIndexName = new List<string>();
			lIndexElem.ForEach(x => lIndexName.AddUnique(x.Attribute("index").Value));
			lock (syncLock)
			{
				_threadGroup.dEngineIndexes.AddOrUpdate(engineName, lIndexName, (k, v) => v.Concat(lIndexName).Distinct().ToList());
			}

			foreach (XElement indexElem in lIndexElem)
			{
				double d;
				string indexName = indexElem.Attribute("index").Value;
				if (_threadGroup.conf.outputIQLSearchRWA)
				{
					//get SearchRWA duration
					//<timing name="SearchRWA" duration="4.39 ms" start="6.78 ms" tid="22" />
					if (GetTimingDuration(indexElem, "SearchRWA", out d)) IQLSearchRWA.Add((engineName, indexName, d));
					else return false;

					//get FullTextSearchRWA duration
					//<timing name='FullTextSearchRWA' duration='4.39 ms' start='5.15 ms' tid='14'/>
					if (indexElem.Descendants("timing").Any(x => Str.EQNC(x.Attribute("name").Value, "FullTextSearchRWA")))
					{
						if (GetTimingDuration(indexElem, "FullTextSearchRWA", out d)) IQLFullTextSearchRWA.Add((engineName, indexName, d));
						else return false;
					}
				}

				if (_threadGroup.conf.outputIQLDBQuery)
				{
					//get ExecuteDBQuery & Fetching DBQuery duration
					//<timing name="ExecuteDBQuery" duration="15.56 ms" start="68.74 ms" tid="22" />
					if (indexElem.Descendants("timing").Any(x => Str.EQNC(x.Attribute("name").Value, "ExecuteDBQuery")))
					{
						if (GetTimingDuration(indexElem, "ExecuteDBQuery", out d)) IQLExecuteDBQuery.Add((engineName, indexName, d));
						else return false;
					}

					//<timing name="Fetching DBQuery" duration="15.62 ms" start="68.69 ms" tid="22" />
					if (indexElem.Descendants("timing").Any(x => Str.EQNC(x.Attribute("name").Value, "Fetching DBQuery")))
					{
						if (GetTimingDuration(indexElem, "Fetching DBQuery", out d)) IQLFetchingDBQuery.Add((engineName, indexName, d));
						else return false;
					}
				}

                if (_threadGroup.conf.outputIQLAcqRLk)
				{
					//get AcqRLk duration
					//<timing name="AcqRLk" duration="0.00 ms" start="6.15 ms" tid="14" />
					if (indexElem.Descendants("timing").Any(x => Str.EQNC(x.Attribute("name").Value, "AcqRLk")))
					{
						if (GetTimingDuration(indexElem, "AcqRLk", out d)) IQLAcqRLk.Add((engineName, indexName, d));
						else return false;
					}
				}
			}

			return true;
		}

		private bool GetFromIQL_Distribution_Correlation_Duration(XElement engineElem, string engineName)
        {
			if (engineElem == null) return false;

			double d;
			//<timing name='distribution(documentlanguages,order=freqdesc,post-group-by=true,merge-groups=true)' duration='0.05 ms' start='61.96 ms' tid='13'/>
			List<XElement> lDistElem = engineElem.Descendants("timing").Where(x => x.Attribute("name").Value.StartsWith("distribution")).ToList();
            if (lDistElem != null || lDistElem.Count != 0)
            {
				List<string> lDistColumn = new List<string>();
				foreach (XElement distElem in lDistElem)
				{
					string dist = Str.Replace(distElem.Attribute("name").Value, " ", "");
					string distAlias = _threadGroup.dDistCorrelAliases.Single(x => Str.EQNC(x.Key, dist)).Value;
					lDistColumn.Add(distAlias);

					if (GetTimingDuration(distElem, out d)) IQLDistribution.Add((engineName, distAlias, d));
					else return false;
				}
				//add to static dictionary of <distribution><list<engines>>
				lock (syncLock)
				{
					_threadGroup.dEngineDistributions.AddOrUpdate(engineName, lDistColumn, (k, v) => v.Concat(lDistColumn).Distinct().ToList());
				}
			}

			//<timing name='correlation(geo,count=10,order=scoredesc,order2=labelasc,labels=true,scores=false,freq=true,basicforms=true,limit=100,post-group-by=true,merge-groups=true)' duration='0.71 ms' start='48.13 ms' tid='13'/>
			List<XElement> lCorrelElem = engineElem.Descendants("timing").Where(x => x.Attribute("name").Value.StartsWith("correlation")).ToList();
            if (lCorrelElem != null || lCorrelElem.Count != 0)
			{
				List<string> lCorrelColumn = new List<string>();
				foreach (XElement correlElem in lCorrelElem)
				{
					string correl = Str.Replace(correlElem.Attribute("name").Value, " ", "");
					string correlAlias = _threadGroup.dDistCorrelAliases.Single(x => Str.EQNC(x.Key ,correl)).Value;
					lCorrelColumn.Add(correlAlias);

					if (GetTimingDuration(correlElem, out d)) IQLCorrelation.Add((engineName, correlAlias, d));
					else return false; 
				}
				//add to static dictionary of <correlation><list<engines>>
				lock (syncLock)
				{
					_threadGroup.dEngineCorrelations.AddOrUpdate(engineName, lCorrelColumn, (k, v) => v.Concat(lCorrelColumn).Distinct().ToList());
				}
			}

			return true;
		}

		private bool GetFromIQL_Engine_Tid_Distinct(XElement engineElem, string engineName)
        {
			if (engineElem == null) return false;

			//<timing name='...' duration='0.00 ms' start='4.55 ms' tid='9'/>
			//get count of all nodes having a distinct tid attribute
			List<XElement> l = engineElem.Descendants().Where(x => x.Attribute("tid") != null).ToList();

			if (l == null)
			{
				Sys.LogWarning($"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] Cannot find [tid] elements in XML [{engineElem}]");
				return false;
			}

			int uniqueTid = l.Select(x => x.Attribute("tid").Value).Distinct().Count();
			dIQLEngineThreadCount.Add(engineName, uniqueTid);

			Sys.Log2(200, $"{{{threadId}}} Thread group [{_threadGroup.name}][{iteration}] GetFromIQL_Engine_Tid_Distinct [{engineName}][{uniqueTid}]");

			return true;
		}

		private bool GetFromIQL_MergeAttributes_Duration(XDocument xInternalQueryLog)
		{
			if (xInternalQueryLog == null) return false;

			//<timing name="MergeAttributes" duration="0.08 ms" start="28.27 ms" tid="14" />
			if (GetTimingDuration(xInternalQueryLog.Root, "MergeAttributes", out double d)) IQLMergeAttributes = d;
			else return false; 
			
			return true;
		}

		private bool GetFromIQL_RFMBoost(XElement RFMBoostElem)
        {
			if (RFMBoostElem == null) return false;
			double d;

			//<timing name='RFM:exact' duration='6.36 ms' start='0.46 ms' tid='24'/>
			if (GetTimingDuration(RFMBoostElem, "RFM:exact", out d)) IQLRFMBoostExact = d;
			else return false;

			//<timing name='RFM:similar' duration='17.03 ms' start='0.40 ms' tid='169'/>
			if (GetTimingDuration(RFMBoostElem, "RFM:similar", out d)) IQLRFMBoostSimilar = d;
			else return false;

			return true;
		}

		private string EngineSRPCToEngineName(string srpcEngineName)
        {
			if (String.IsNullOrEmpty(srpcEngineName)) return null;

			string host = Str.ParseFromTo(srpcEngineName, "srpc://", ":");
			int port = int.Parse(Str.ParseFromLastSep(srpcEngineName, ':'));
			
			//get engine name from EngineCustomStatus (using <host>:<port>)
			if (_threadGroup.conf.enginesStatus.Exists(x => Str.EQNC(x.Host, host) && x.Port == port))
            {
				return _threadGroup.conf.enginesStatus.Single(x => Str.EQNC(x.Host, host) && x.Port == port).Name;
            }

			//cannot find the engine (using <host>:<port>), host can be refered as 'localhost' in EngineCustomStatus if the command runs on the same node as the Engine(s)
			if (Str.EQNC(host, _commandHost))	//command and engine have the same host
			{
				string optHost = this._threadGroup.engineBenchmark.Command.Identity.Node.HostOptimized;	//refer as localhost
				//get engine name from EngineCustomStatus (using <host>:<port>)
				if (_threadGroup.conf.enginesStatus.Exists(x => Str.EQNC(x.Host, optHost) && x.Port == port))
				{
					return _threadGroup.conf.enginesStatus.Single(x => Str.EQNC(x.Host, optHost) && x.Port == port).Name;
				}
			}
			return srpcEngineName;
		}

		public bool SetCursorSizeBreakdown(BenchmarkQuery query)
		{
			if (query.cursorSizeBreakdown == null) return false;
			this.dCursorSizeBreakdown = query.cursorSizeBreakdown;
			return true;
		}

		public List<KeyValuePair<string, long>> GetCursorSize()
		{
			return this._threadGroup.conf.outputCursorSizeEmptyColumns ? this.dCursorSizeBreakdown.OrderBy(x => x.Key).ToList() : this.dCursorSizeBreakdown.Where(x => x.Value > 0).OrderBy(x => x.Key).ToList();
		}

		public bool DumpInternalQueryLog(string tgName, int iteration, BenchmarkQuery query)
		{
			Stopwatch swDump = new Stopwatch();
			swDump.Start();

			Thread threadGroupThread = Thread.CurrentThread;
			int threadGroupThreadId = threadGroupThread.ManagedThreadId;

			if (processingTime < this._threadGroup.conf.dumpIQLMinProcessingTime)
			{
				Sys.Log2(50, $"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Skip InternalQueryLog dump, ProcessingTime [{processingTime.ToString()}] < Minimum query processing time to dump Internal Query Log XML [{this._threadGroup.conf.dumpIQLMinProcessingTime.ToString()}]");
				return true;
			}

			if (query.internalQueryLog == null)
			{
				Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Cannot dump InternalQueryLog. Are you missing <internalquerylog> in your select statement ?");
				return false;
			}

			if (Str.IsEmpty(query.internalQueryLog))
			{
				Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Empty InternalQueryLog");
			}

			string dumpInternalQueryLogFilePath = Toolbox.GetOutputFilePath(_threadGroup.conf.outputFolderPath ,$"internalquerylog_{tgName}_{iteration}", "xml", "InternalQueryLog");

			if (Toolbox.DumpFile(dumpInternalQueryLogFilePath, query.internalQueryLog))
			{
				swDump.Stop();
				Sys.Log($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Create InternalQueryLog XML dump [{dumpInternalQueryLogFilePath}] [{Sys.TimerGetText(swDump.ElapsedMilliseconds)}]");
				return true;
			}
			return false;
		}

		public bool DumpInternalQueryAnalysis(string tgName, int iteration, BenchmarkQuery query)
		{
			Stopwatch swDump = new Stopwatch();

			Thread threadGroupThread = Thread.CurrentThread;
			int threadGroupThreadId = threadGroupThread.ManagedThreadId;

			if (processingTime < this._threadGroup.conf.dumpIQAMinProcessingTime)
			{
				Sys.Log2(50, $"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Skip InternalQueryAnalysis dump, ProcessingTime [{processingTime}] < Minimum query processing time to dump Internal Query Analysis XML [{this._threadGroup.conf.dumpIQAMinProcessingTime.ToString()}]");
				return true;
			}

			if (query.dInternalQueryAnalysis.Count == 0)
			{
				Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Cannot dump InternalQueryAnalysis. Are you missing <internalqueryanalysis> in your select statement ? <internalqueryanalysis> is only returned when you have a <where text contains '...'> clause");
				return false;
			}

			bool dumpStatus = true;
            foreach (string IQAAttributeName in query.dInternalQueryAnalysis.Keys)
            {
				swDump.Start();

				string engineName = Str.Empty;
                if (Str.Contains(IQAAttributeName, ".srpc://"))
                {
					//internalqueryanalysis.srpc://note157:10401
					string engineSRPCName = Str.ParseFromSep(IQAAttributeName, '.');
					//resolve name
					engineName = "_" + EngineSRPCToEngineName(engineSRPCName);
				}
				
				//file path
				string dumpInternalQueryAnalysisFilePath = Toolbox.GetOutputFilePath(_threadGroup.conf.outputFolderPath, $"internalqueryanalysis_{tgName}_{iteration}{engineName}", "xml", "InternalQueryAnalysis");

				//get InternalQueryAnalysis value
				query.dInternalQueryAnalysis.TryGetValue(IQAAttributeName, out string internalqueryanalysis);
                if (String.IsNullOrEmpty(internalqueryanalysis))
                {
					Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Empty InternalQueryAnalysis [{IQAAttributeName}]");
					continue;
                }

				if (Toolbox.DumpFile(dumpInternalQueryAnalysisFilePath, internalqueryanalysis))
				{
					swDump.Stop();
					Sys.Log($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Create InternalQueryAnalysis XML dump [{dumpInternalQueryAnalysisFilePath}] [{Sys.TimerGetText(swDump.ElapsedMilliseconds)}]");
					swDump.Reset();
				}
				else dumpStatus = false;
			}

			return dumpStatus;
		}

		public string QueryOutputCSVHeader(char separator = ';')
		{
			ListStr lHeaders = new ListStr();
			lHeaders.Add(GetOutputGenericCSVHeader());
			if (this._threadGroup.conf.outputSQLQuery)
			{
				lHeaders.Add("SQL");
			}
			if (this._threadGroup.conf.outputQueryTimers)
			{
				lHeaders.Add("TotalQueryTime (ms)");
				lHeaders.Add("ProcessingTime (ms)");
				lHeaders.Add("RowFetchTime (ms)");
				lHeaders.Add("ReadCursor (ms)");
			}
			if (this._threadGroup.conf.outputQueryInfo)
			{
				lHeaders.Add("CacheHit");
				lHeaders.Add("MatchingRowCount");
				lHeaders.Add("PostGroupByMatchingRowCount");
			}
			if (this._threadGroup.conf.outputClientTimers)
			{
				lHeaders.Add("ClientFromPool (ms)");
				lHeaders.Add("ClientToPool (ms)");
			}
			if (this._threadGroup.conf.outputCurosrNetworkAndDeserializationTimer)
			{
				lHeaders.Add("Curosr Network and Deserialization (ms)");
			}
			if (this._threadGroup.conf.outputParameters)
			{
				lHeaders.Add(dParams.Select(x => x.Key).ToArray());
			}
			if (this._threadGroup.conf.outputIQL)
			{
				lHeaders.Add(GetInternalQueryLogCSVHeader(separator));
			}
			return lHeaders.ToStr(separator);
		}

		public string QueryOutputCSVRow(char separator = ';')
		{
			ListStr lColumns = new ListStr();
			lColumns.Add(GetOutputGenericGenericCSVRow());
			if (this._threadGroup.conf.outputSQLQuery)
			{
				//SQL
				lColumns.Add(sql.ToString());
			}
			if (this._threadGroup.conf.outputQueryTimers)
			{
				//TotalQueryTime (ms)
				if (querySuccess) lColumns.Add(totalQueryTime.ToString()); else lColumns.Add(Str.Empty);
				//ProcessingTime (ms)
				if (querySuccess) lColumns.Add(processingTime.ToString()); else lColumns.Add(Str.Empty);
				//Rowfetchtime (ms)
				if (querySuccess) lColumns.Add(rowFetchTime.ToString()); else lColumns.Add(Str.Empty);
				//ReadCursor (ms)
				if (querySuccess) lColumns.Add(readCursor.ToString()); else lColumns.Add(Str.Empty);
			}
			if (this._threadGroup.conf.outputQueryInfo)
			{
				//Cachehit
				if (querySuccess) lColumns.Add(cacheHit.ToString()); else lColumns.Add(Str.Empty);
				//Matchingrowcount
				if (querySuccess) lColumns.Add(matchingRowCount.ToString()); else lColumns.Add(Str.Empty);
				//PostGroupByMatchingRowCount
				if (querySuccess) lColumns.Add(postGroupByMatchingRowCount.ToString()); else lColumns.Add(Str.Empty);
			}
			if (this._threadGroup.conf.outputClientTimers)
			{
				//ClientFromPool (ms)
				lColumns.Add(clientFromPool.ToString());
				//ClientToPool (ms)
				lColumns.Add(clientToPool.ToString());
			}
			if (this._threadGroup.conf.outputCurosrNetworkAndDeserializationTimer)
			{
				//Curosr Network and Deserialization (ms)
				if (querySuccess) lColumns.Add(curosrNetworkAndDeserialization.ToString()); else lColumns.Add(Str.Empty);
			}
			if (this._threadGroup.conf.outputParameters)
			{
				//params
				lColumns.Add(dParams.Select(x => x.Value).ToArray());
			}
			if (this._threadGroup.conf.outputIQL)
			{
				lColumns.Add(GetInternalQueryLogCSVRow(separator));
			}
			return lColumns.ToStr(separator);
		}

		private ListStr GetOutputGenericCSVHeader()
		{
			ListStr lHeaders = new ListStr();
			lHeaders.Add("Thread Group Name");
			lHeaders.Add("Iteration");
			lHeaders.Add("Date start");
			lHeaders.Add("Date end");
			lHeaders.Add("Success");
			lHeaders.Add("Engine name");
			lHeaders.Add("Cursor size MB");
			return lHeaders;
		}

		private ListStr GetOutputGenericGenericCSVRow()
		{
			ListStr lColumns = new ListStr();
			//thread group name
			lColumns.Add(_threadGroup.name.ToString());
			//Iteration
			lColumns.Add(iteration.ToString());
			//Date start
			lColumns.Add(dStart.ToString("yyyy-MM-dd HH:mm:ss,fff"));
			//Date end
			lColumns.Add(dEnd.ToString("yyyy-MM-dd HH:mm:ss,fff"));
			//Success
			lColumns.Add(querySuccess.ToString());
			//Engine name
			lColumns.Add(engineClientName.ToString());
			//Cursor size
			if(querySuccess) lColumns.Add(cursorSizeMB.ToString()); else lColumns.Add(Str.Empty);
			return lColumns;
		}

		private string GetInternalQueryLogCSVHeader(char separator = ';')
		{
			ListStr lHeaders = new ListStr();
			if (this._threadGroup.conf.outputIQLSearchRWA)
			{
				lHeaders.Add(_threadGroup.lSortedEngineIndex.Select(x => $"{x.index}[{x.engine}][SearchRWA]").ToArray());
				lHeaders.Add(_threadGroup.lSortedEngineIndex.Select(x => $"{x.index}[{x.engine}][FullTextSearchRWA]").ToArray());
			}
			if (this._threadGroup.conf.outputIQLDBQuery)
			{
				lHeaders.Add(_threadGroup.lSortedEngineIndex.Select(x => $"{x.index}[{x.engine}][ExecuteDBQuery]").ToArray());
				lHeaders.Add(_threadGroup.lSortedEngineIndex.Select(x => $"{x.index}[{x.engine}][FetchingDBQuery]").ToArray());
			}
            if (this._threadGroup.conf.outputIQLAcqRLk)
            {
				lHeaders.Add(_threadGroup.lSortedEngineIndex.Select(x => $"{x.index}[{x.engine}][AcqRLk]").ToArray());
			}
			if (this._threadGroup.conf.outputIQLDistributionsCorrelations)
			{
				lHeaders.Add(_threadGroup.lSortedEngineDistribution.Select(x => $"{x.distribution}[{x.engine}][Distribution]").ToArray());
				lHeaders.Add(_threadGroup.lSortedEngineCorrelation.Select(x => $"{x.correlation}[{x.engine}][Correlation]").ToArray());
			}
			if (this._threadGroup.conf.outputIQLHeader)
			{
				lHeaders.Add("AcqMRdLk");
				lHeaders.Add("AcqDBRdLk");
				lHeaders.Add("NetworkNotificationToWorkerStart");
				lHeaders.Add("MsgDeserialize");
				lHeaders.Add("QueryProcessorParse");
			}
			if (this._threadGroup.conf.outputRFMBoost)
			{
				lHeaders.Add("RFM:exact");
				lHeaders.Add("RFM:similar");
			}
			if (this._threadGroup.conf.outputIQLBrokering)
			{
				lHeaders.Add("Broker Engine");
				lHeaders.Add("Client Engine(s)");
				lHeaders.Add("MergeAttributes");
			}
			if (this._threadGroup.conf.outputIQLThreadCount)
			{
				lHeaders.Add("Header threads");
				lHeaders.Add(_threadGroup.dEngineIndexes.Select(x => $"Threads [{x.Key}]").ToArray());
				lHeaders.Add("Total threads");
			}
			return lHeaders.ToStr(separator);
		}

		private string GetInternalQueryLogCSVRow(char separator = ';')
		{
			ListStr lColumns = new ListStr();
			try
			{
				if (this._threadGroup.conf.outputIQLSearchRWA)
				{
					//SearchRWA
					foreach ((string index, string engine) o in _threadGroup.lSortedEngineIndex)
					{
						(string engine, string index, double duration) r = IQLSearchRWA.SingleOrDefault(x => Str.EQNC(x.engineName, o.engine) && Str.EQNC(x.index, o.index));
						if (String.IsNullOrEmpty(r.engine) || r.duration < 0)
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(r.duration.ToString());
					}
					//FullTextSearchRWA
					foreach ((string index, string engine) o in _threadGroup.lSortedEngineIndex)
					{
						(string engine, string index, double duration) r = IQLFullTextSearchRWA.SingleOrDefault(x => Str.EQNC(x.engineName, o.engine) && Str.EQNC(x.index, o.index));
						if (String.IsNullOrEmpty(r.engine) || r.duration < 0)
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(r.duration.ToString());
					}
				}
				if (this._threadGroup.conf.outputIQLDBQuery)
				{
					//ExecuteDBQuery
					foreach ((string index, string engine) o in _threadGroup.lSortedEngineIndex)
					{
						(string engine, string index, double duration) r = IQLExecuteDBQuery.SingleOrDefault(x => Str.EQNC(x.engineName, o.engine) && Str.EQNC(x.index, o.index));
						if (String.IsNullOrEmpty(r.engine) || r.duration < 0)
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(r.duration.ToString());
					}
					//FetchingDBQuery
					foreach ((string index, string engine) o in _threadGroup.lSortedEngineIndex)
					{
						(string engine, string index, double duration) r = IQLFetchingDBQuery.SingleOrDefault(x => Str.EQNC(x.engineName, o.engine) && Str.EQNC(x.index, o.index));
						if (String.IsNullOrEmpty(r.engine) || r.duration < 0)
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(r.duration.ToString());
					}
				}
                if (this._threadGroup.conf.outputIQLAcqRLk)
                {
					//AcqRLk
					foreach ((string index, string engine) o in _threadGroup.lSortedEngineIndex)
					{
						(string engine, string index, double duration) r = IQLAcqRLk.SingleOrDefault(x => Str.EQNC(x.engineName, o.engine) && Str.EQNC(x.index, o.index));
						if (String.IsNullOrEmpty(r.engine) || r.duration < 0)
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(r.duration.ToString());
					}
				}
				if (this._threadGroup.conf.outputIQLDistributionsCorrelations)
				{
					//Distribution
					foreach ((string distribution, string engine) o in _threadGroup.lSortedEngineDistribution)
					{
						(string engine, string distribution, double duration) r = IQLDistribution.SingleOrDefault(x => Str.EQNC(x.engineName, o.engine) && Str.EQNC(x.distribution, o.distribution));
						if (String.IsNullOrEmpty(r.engine) || r.duration < 0)
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(r.duration.ToString());
					}
					//Correlation
					foreach ((string correlation, string engine) o in _threadGroup.lSortedEngineCorrelation)
					{
						(string engine, string correlation, double duration) r = IQLCorrelation.SingleOrDefault(x => Str.EQNC(x.engineName, o.engine) && Str.EQNC(x.correlation, o.correlation));
						if (String.IsNullOrEmpty(r.engine) || r.duration < 0)
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(r.duration.ToString());
					}
				}
				if (this._threadGroup.conf.outputIQLHeader)
				{
					//AcqMRdLk duration
					if (querySuccess) lColumns.Add(IQLAcqMRdLk.ToString()); else lColumns.Add(Str.Empty);
					//AcqDBRdLk
					if (querySuccess) lColumns.Add(IQLAcqDBRdLk.ToString()); else lColumns.Add(Str.Empty);
					//NetworkNotificationToWorkerStart
					if (querySuccess) lColumns.Add(IQLNetworkNotificationToWorkerStart.ToString()); else lColumns.Add(Str.Empty);
					//MsgDeserialize
					if (querySuccess) lColumns.Add(IQLMsgDeserialize.ToString()); else lColumns.Add(Str.Empty);
					//QueryProcessorParse
					if (querySuccess) lColumns.Add(IQLQueryProcessorParse.ToString()); else lColumns.Add(Str.Empty);
				}
				if (this._threadGroup.conf.outputRFMBoost)
				{
					//RFM Boost
					//Exact
					if (querySuccess) lColumns.Add(IQLRFMBoostExact.ToString()); else lColumns.Add(Str.Empty);
					//Similar
					if (querySuccess) lColumns.Add(IQLRFMBoostSimilar.ToString()); else lColumns.Add(Str.Empty);
				}
				if (this._threadGroup.conf.outputIQLBrokering)
				{
					//Broker Engine
					lColumns.Add(IQLBrokerEngine);
					//Client Engine(s)
					lColumns.Add(string.Join(",", IQLBorkerClients.ToArray()));
					//MergeAttributes
					if (querySuccess && IQLBorkerClients.Count > 0) lColumns.Add(IQLMergeAttributes.ToString()); else lColumns.Add(Str.Empty);
				}
				if (this._threadGroup.conf.outputIQLThreadCount)
				{
					//threads missing tid - fixed V11.5.0.1002 - ES-12232
					//Header threads
					if (querySuccess) lColumns.Add(IQLHeaderThreadCount.ToString()); else lColumns.Add(Str.Empty);
					//Threads per engine
					foreach (string engine in _threadGroup.dEngineIndexes.Keys)
					{
						if (!dIQLEngineThreadCount.ContainsKey(engine))
							lColumns.Add(Str.Empty);
						else
							lColumns.Add(dIQLEngineThreadCount.Get(engine).ToString());
					}
					//Total threads
					//TODO - if no Engine tag, IQLHeaderThreadCount will count twice
					if (querySuccess) lColumns.Add((IQLHeaderThreadCount + dIQLEngineThreadCount.Sum(x => x.Value)).ToString()); else lColumns.Add(Str.Empty);
				}
			}
            catch (Exception e)
			{
				//DEBUG
				Sys.LogError(e);

				Sys.Log("-------------------------------------------------------------------------------------------");
				Sys.Log("_threadGroup.cached_lSortedEngineIndex");
				foreach ((string index, string engine) o in _threadGroup.lSortedEngineIndex)
				{
					Sys.Log($"engine = [{o.engine}] index = [{o.index}]");
				}
				Sys.Log("IQLSearchRWA");
				foreach ((string engine, string index, double duration) o in IQLSearchRWA)
				{
					Sys.Log($"engine = [{o.engine}] index = [{o.index}] duration = [{o.duration}]");
				}
				Sys.Log("IQLFullTextSearchRWA");
				foreach ((string engine, string index, double duration) o in IQLFullTextSearchRWA)
				{
					Sys.Log($"engine = [{o.engine}] index = [{o.index}] duration = [{o.duration}]");
				}
				Sys.Log("IQLExecuteDBQuery");
				foreach ((string engine, string index, double duration) o in IQLExecuteDBQuery)
				{
					Sys.Log($"engine = [{o.engine}] index = [{o.index}] duration = [{o.duration}]");
				}
				Sys.Log("IQLFetchingDBQuery");
				foreach ((string engine, string index, double duration) o in IQLFetchingDBQuery)
				{
					Sys.Log($"engine = [{o.engine}] index = [{o.index}] duration = [{o.duration}]");
				}
				Sys.Log("IQLAcqRLk");
				foreach ((string engine, string index, double duration) o in IQLAcqRLk)
				{
					Sys.Log($"engine = [{o.engine}] index = [{o.index}] duration = [{o.duration}]");
				}
				Sys.Log("_threadGroup.cached_lSortedEngineDistribution");
				foreach ((string distribution, string engine) o in _threadGroup.lSortedEngineDistribution)
				{
					Sys.Log($"engine = [{o.engine}] distribution = [{o.distribution}]");
				}
				Sys.Log("IQLDistribution");
				foreach ((string engine, string distribution, double duration) o in IQLDistribution)
				{
					Sys.Log($"engine = [{o.engine}] distribution = [{o.distribution}] duration = [{o.duration}]");
				}
				Sys.Log("_threadGroup.cached_lSortedEngineCorrelation");
				foreach ((string correlation, string engine) o in _threadGroup.lSortedEngineCorrelation)
				{
					Sys.Log($"engine = [{o.engine}] correlation = [{o.correlation}]");
				}
				Sys.Log("IQLCorrelation");
				foreach ((string engine, string correlation, double duration) o in IQLCorrelation)
				{
					Sys.Log($"engine = [{o.engine}] correlation = [{o.correlation}] duration = [{o.duration}]");
				}
				Sys.Log("-------------------------------------------------------------------------------------------");
			}
			
			return lColumns.ToStr(separator);
		}

		public string CursorSizeCSVHeader(char separator = ';')
		{
			ListStr lHeaders = new ListStr();
			lHeaders.Add(GetOutputGenericCSVHeader());
			GetCursorSize().ForEach(x => lHeaders.Add(x.Key));
			return lHeaders.ToStr(separator);
		}

		public string CursorSizeCSVRow(char separator = ';')
		{
			ListStr lColumns = new ListStr();
			lColumns.Add(GetOutputGenericGenericCSVRow());
			GetCursorSize().ForEach(x => lColumns.Add(x.Value.ToString()));
			return lColumns.ToStr(separator);
		}
	}

}
