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
		public int id { get; private set; }
		//info
		public DateTime dStart { get; private set; }
		public DateTime dEnd { get; private set; }
		public string sql { get; private set; }
		public int threadId { get; private set; }
		public bool success { get; private set; }
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

		//Cursor Size Breakdown
		public Dictionary<string, long> dCursorSizeBreakdown { get; private set; } = new Dictionary<string, long>();

		public ThreadGroupOutput(ThreadGroup threadGroup, int id, int threadId, DateTime start, DateTime end)
		{
			this._threadGroup = threadGroup;
			this._commandHost = this._threadGroup.engineBenchmark.Command.Identity.Node.Host;
			this.id = id;
			this.threadId = threadId;
			this.dStart = start;
			this.dEnd = end;
		}

		public void SetSuccess(bool success)
		{
			this.success = success;
		}

		public void SetSQL(string sql)
		{
			this.sql = sql;
		}

		public void SetInfo(string engineName, Dictionary<string, string> dParams, long cursorSize)
		{
			this.engineClientName = engineName;
			this.dParams = dParams;
			this.cursorSize = cursorSize;
		}

		public void SetClientTimers(long clientFromPool, long clientToPool)
		{
			this.clientFromPool = clientFromPool;
			this.clientToPool = clientToPool;
		}

		public void SetQueryTimers(double processingTime, long cachehit, double rowfetchtime, long matchingrowcount, long postGroupByMatchingRowCount, long totalQueryTime, long readCursor)
		{
			this.processingTime = processingTime;
			this.cacheHit = cachehit;
			this.rowFetchTime = rowfetchtime;
			this.matchingRowCount = matchingrowcount;
			this.postGroupByMatchingRowCount = postGroupByMatchingRowCount;
			this.totalQueryTime = totalQueryTime;
			this.readCursor = readCursor;
		}

		public void SetCursorNetworkAndDeserialization(double curosrNetworkAndDeserialization)
		{
			this.curosrNetworkAndDeserialization = curosrNetworkAndDeserialization;
		}

		public void SetInternalQueryLog(string intquerylog)
		{
			if (String.IsNullOrEmpty(intquerylog)) return;

			double d = 0;
			XDocument xInternalQueryLog = XDocument.Parse(intquerylog);
			if (xInternalQueryLog == null)
			{
				Sys.LogError($"Cannot parse InternalQueryLog [{intquerylog}]");
				return;
			}

			//AcqMRdLk duration & start - AcqDBRdLk - NetworkNotificationToWorkerStart - MsgDeserialize - QueryProcessorParse
			if (_threadGroup.conf.outputIQLHeader) GetFromIQL_Header_Duration(xInternalQueryLog);
			if (_threadGroup.conf.outputIQLThreadCount) GetFromIQL_Header_Tid_Distinct(xInternalQueryLog);

				//no brokering, no engine tag
			if (xInternalQueryLog.Descendants("Engine").Count() == 0)
			{
				string engineName = engineClientName;

				//indexes list
				//<IndexSearch index="myIndex">
				List<XElement> lIndexElem = xInternalQueryLog.Descendants("IndexSearch").ToList();

				//SearchRWA & DBQuery
				if (_threadGroup.conf.outputIQLSearchRWA || _threadGroup.conf.outputIQLDBQuery) GetFromIQL_SearchRWA_DBQuery_Duration(lIndexElem, engineName);

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

					//SearchRWA & DBQuery
					if (_threadGroup.conf.outputIQLSearchRWA || _threadGroup.conf.outputIQLDBQuery) GetFromIQL_SearchRWA_DBQuery_Duration(lIndexElem, engineName);

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
		}	

		private void GetFromIQL_Header_Duration(XDocument xInternalQueryLog)
        {
			if (xInternalQueryLog == null) return;

			double d;
			//<timing name='AcqMRdLk' duration='0.00 ms' start='2.68 ms' tid='17'/>
			if (double.TryParse(Str.ParseToSep(xInternalQueryLog.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "AcqMRdLk")).Attribute("duration").Value, ' '), out d)) IQLAcqMRdLk = d;
			//<timing name='AcqDBRdLk' duration='0.00 ms' start='2.70 ms' tid='17'/>
			if (double.TryParse(Str.ParseToSep(xInternalQueryLog.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "AcqDBRdLk")).Attribute("duration").Value, ' '), out d)) IQLAcqDBRdLk = d;
			//<timing name='NetworkNotificationToWorkerStart' duration='0.21 ms' start='0.21 ms' tid='17'/>
			if (double.TryParse(Str.ParseToSep(xInternalQueryLog.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "NetworkNotificationToWorkerStart")).Attribute("duration").Value, ' '), out d)) IQLNetworkNotificationToWorkerStart = d;
			//<timing name='MsgDeserialize' duration='0.02 ms' start='0.23 ms' tid='17'/>
			if (double.TryParse(Str.ParseToSep(xInternalQueryLog.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "MsgDeserialize")).Attribute("duration").Value, ' '), out d)) IQLMsgDeserialize = d;
			//<timing name='QueryProcessor::Parse' duration='2.32 ms' start='0.33 ms' tid='17'/>
			if (double.TryParse(Str.ParseToSep(xInternalQueryLog.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "QueryProcessor::Parse")).Attribute("duration").Value, ' '), out d)) IQLQueryProcessorParse = d;
		}

		private void GetFromIQL_Header_Tid_Distinct(XDocument xInternalQueryLog)
		{
			if (xInternalQueryLog == null) return;

			string[] nodesTimingToMatch = { "AcqMRdLk", "AcqDBRdLk", "NetworkNotificationToWorkerStart", "MsgDeserialize", "QueryProcessor::Parse" };
			IQLHeaderThreadCount = xInternalQueryLog.Descendants("timing").Where(x => Str.EQNCN(x.Attribute("name").Value, nodesTimingToMatch) && x.Attribute("tid") != null).Select(x => x.Attribute("tid").Value).Distinct().Count();
		}

		//SearchRWA - FullTextSearchRWA - ExecuteDBQuery - Fetching DBQuery
		private void GetFromIQL_SearchRWA_DBQuery_Duration(List<XElement> lIndexElem, string engineName)
        {
			if (lIndexElem == null || lIndexElem.Count == 0) return;

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
					if (double.TryParse(Str.ParseToSep(indexElem.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "SearchRWA")).Attribute("duration").Value, ' '), out d)) IQLSearchRWA.Add((engineName, indexName, d));

					//get FullTextSearchRWA duration
					//<timing name='FullTextSearchRWA' duration='4.39 ms' start='5.15 ms' tid='14'/>
					if (double.TryParse(Str.ParseToSep(indexElem.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "FullTextSearchRWA")).Attribute("duration").Value, ' '), out d)) IQLFullTextSearchRWA.Add((engineName, indexName, d));
					
				}

				if (_threadGroup.conf.outputIQLDBQuery)
				{
					//get ExecuteDBQuery & Fetching DBQuery duration
					//<timing name="ExecuteDBQuery" duration="15.56 ms" start="68.74 ms" tid="22" />
					if (indexElem.Descendants("timing").Any(x => Str.EQNC(x.Attribute("name").Value, "ExecuteDBQuery")))
					{
						if (double.TryParse(Str.ParseToSep(indexElem.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "ExecuteDBQuery")).Attribute("duration").Value, ' '), out d)) IQLExecuteDBQuery.Add((engineName, indexName, d));
					}

					//<timing name="Fetching DBQuery" duration="15.62 ms" start="68.69 ms" tid="22" />
					if (indexElem.Descendants("timing").Any(x => Str.EQNC(x.Attribute("name").Value, "Fetching DBQuery")))
					{
						if (double.TryParse(Str.ParseToSep(indexElem.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "Fetching DBQuery")).Attribute("duration").Value, ' '), out d)) IQLFetchingDBQuery.Add((engineName, indexName, d));
					}
				}
			}
		}

		private void GetFromIQL_Distribution_Correlation_Duration(XElement engineElem, string engineName)
        {
			if (engineElem == null) return;

			double d;
			//<timing name='distribution(documentlanguages,order=freqdesc,post-group-by=true,merge-groups=true)' duration='0.05 ms' start='61.96 ms' tid='13'/>
			List<XElement> lDistElem = engineElem.Descendants("timing").Where(x => x.Attribute("name").Value.StartsWith("distribution")).ToList();
            if (lDistElem != null || lDistElem.Count != 0)
            {
				List<string> lDistColumn = new List<string>();
				foreach (XElement distElem in lDistElem)
				{
					string disColumn = Str.ParseFromTo(distElem.Attribute("name").Value, "(", ",");
					lDistColumn.Add(disColumn);
					if (double.TryParse(Str.ParseToSep(distElem.Attribute("duration").Value, ' '), out d)) IQLDistribution.Add((engineName, disColumn, d));
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
					string correlColumn = Str.ParseFromTo(correlElem.Attribute("name").Value, "(", ",");
					lCorrelColumn.Add(correlColumn);
					if (double.TryParse(Str.ParseToSep(correlElem.Attribute("duration").Value, ' '), out d)) IQLCorrelation.Add((engineName, correlColumn, d));
				}
				//add to static dictionary of <correlation><list<engines>>
				lock (syncLock)
				{
					_threadGroup.dEngineCorrelations.AddOrUpdate(engineName, lCorrelColumn, (k, v) => v.Concat(lCorrelColumn).Distinct().ToList());
				}
			}			
		}

		private void GetFromIQL_Engine_Tid_Distinct(XElement engineElem, string engineName)
        {
			if (engineElem == null) return;

			//<timing name='...' duration='0.00 ms' start='4.55 ms' tid='9'/>
			//get count of all nodes having a distinct tid attribute
			int uniqueTid = engineElem.Descendants().Where(x => x.Attribute("tid") != null).Select(x => x.Attribute("tid").Value).Distinct().Count();
			dIQLEngineThreadCount.Add(engineName, uniqueTid);
		}

		private void GetFromIQL_MergeAttributes_Duration(XDocument xInternalQueryLog)
		{
			if (xInternalQueryLog == null) return;

			//<timing name="MergeAttributes" duration="0.08 ms" start="28.27 ms" tid="14" />
			if (double.TryParse(Str.ParseToSep(xInternalQueryLog.Descendants("timing").Single(x => Str.EQNC(x.Attribute("name").Value, "MergeAttributes")).Attribute("duration").Value, ' '), out double d)) IQLMergeAttributes = d;
		}

		private string EngineSRPCToEngineName(string srpcEngineName)
        {
			string host = Str.ParseFromTo(srpcEngineName, "srpc://", ":");
			int port = int.Parse(Str.ParseFromLastSep(srpcEngineName, ':'));
			
			if (String.IsNullOrEmpty(srpcEngineName)) return null;

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

		public void SetCursorSizeBreakdown(Dictionary<string, long> cursorSizeBreakdown)
		{
			this.dCursorSizeBreakdown = cursorSizeBreakdown;
		}

		public List<KeyValuePair<string, long>> GetCursorSize()
		{
			return this._threadGroup.conf.outputCursorSizeEmptyColumns ? this.dCursorSizeBreakdown.OrderBy(x => x.Key).ToList() : this.dCursorSizeBreakdown.Where(x => x.Value > 0).OrderBy(x => x.Key).ToList();
		}

		public bool DumpInternalQueryLog(string tgName, string internalQueryLog, int iteration, double processingTime)
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

			if (internalQueryLog == null)
			{
				Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Cannot dump InternalQueryLog. Are you missing <internalquerylog> in your select statement ?");
				return false;
			}

			if (Str.IsEmpty(internalQueryLog))
			{
				Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Empty InternalQueryLog");
			}

			string dumpInternalQueryLogFilePath = Toolbox.GetOutputFilePath(_threadGroup.conf.outputFolderPath ,$"internalquerylog_{tgName}_{iteration}", "xml", "InternalQueryLog");

			if (Toolbox.DumpFile(dumpInternalQueryLogFilePath, internalQueryLog))
			{
				swDump.Stop();
				Sys.Log($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Create InternalQueryLog XML dump [{dumpInternalQueryLogFilePath}] [{Sys.TimerGetText(swDump.ElapsedMilliseconds)}]");
				return true;
			}
			return false;
		}

		public bool DumpInternalQueryAnalysis(string tgName, string internalQueryAnalysis, int iteration, double processingTime)
		{
			Stopwatch swDump = new Stopwatch();
			swDump.Start();

			Thread threadGroupThread = Thread.CurrentThread;
			int threadGroupThreadId = threadGroupThread.ManagedThreadId;

			if (processingTime < this._threadGroup.conf.dumpIQAMinProcessingTime)
			{
				Sys.Log2(50, $"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Skip InternalQueryAnalysis dump, ProcessingTime [{processingTime}] < Minimum query processing time to dump Internal Query Analysis XML [{this._threadGroup.conf.dumpIQAMinProcessingTime.ToString()}]");
				return true;
			}

			if (internalQueryAnalysis == null)
			{
				Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Cannot dump InternalQueryAnalysis. Are you missing <internalqueryanalysis> in your select statement ? <internalqueryanalysis> is only returned when you have a <where text contains '...'> clause");
				return false;
			}

			if (Str.IsEmpty(internalQueryAnalysis))
			{
				Sys.LogWarning($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Empty InternalQueryAnalysis");
			}

			string dumpInternalQueryAnalysisFilePath = Toolbox.GetOutputFilePath(_threadGroup.conf.outputFolderPath, $"internalqueryanalysis_{tgName}_{iteration}", "xml", "InternalQueryAnalysis");

			if (Toolbox.DumpFile(dumpInternalQueryAnalysisFilePath, internalQueryAnalysis))
			{
				swDump.Stop();
				Sys.Log($"{{{threadGroupThreadId}}} Thread group [{tgName}][{iteration}] Create InternalQueryAnalysis XML dump [{dumpInternalQueryAnalysisFilePath}] [{Sys.TimerGetText(swDump.ElapsedMilliseconds)}]");
				return true;
			}
			return false;
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
				lHeaders.Add(dParams.Select(x => "$" + x.Key + "$").ToArray());
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
				if (success) lColumns.Add(totalQueryTime.ToString()); else lColumns.Add(Str.Empty);
				//ProcessingTime (ms)
				if (success) lColumns.Add(processingTime.ToString()); else lColumns.Add(Str.Empty);
				//Rowfetchtime (ms)
				if (success) lColumns.Add(rowFetchTime.ToString()); else lColumns.Add(Str.Empty);
				//ReadCursor (ms)
				if (success) lColumns.Add(readCursor.ToString()); else lColumns.Add(Str.Empty);
			}
			if (this._threadGroup.conf.outputQueryInfo)
			{
				//Cachehit
				if (success) lColumns.Add(cacheHit.ToString()); else lColumns.Add(Str.Empty);
				//Matchingrowcount
				if (success) lColumns.Add(matchingRowCount.ToString()); else lColumns.Add(Str.Empty);
				//PostGroupByMatchingRowCount
				if (success) lColumns.Add(postGroupByMatchingRowCount.ToString()); else lColumns.Add(Str.Empty);
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
				if (success) lColumns.Add(curosrNetworkAndDeserialization.ToString()); else lColumns.Add(Str.Empty);
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
			lColumns.Add(id.ToString());
			//Date start
			lColumns.Add(dStart.ToString("yyyy-MM-dd HH:mm:ss,fff"));
			//Date end
			lColumns.Add(dEnd.ToString("yyyy-MM-dd HH:mm:ss,fff"));
			//Success
			lColumns.Add(success.ToString());
			//Engine name
			lColumns.Add(engineClientName.ToString());
			//Cursor size
			if(success) lColumns.Add(cursorSizeMB.ToString()); else lColumns.Add(Str.Empty);
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
				if (success) lColumns.Add(IQLAcqMRdLk.ToString()); else lColumns.Add(Str.Empty);
				//AcqDBRdLk
				if (success) lColumns.Add(IQLAcqDBRdLk.ToString()); else lColumns.Add(Str.Empty);
				//NetworkNotificationToWorkerStart
				if (success) lColumns.Add(IQLNetworkNotificationToWorkerStart.ToString()); else lColumns.Add(Str.Empty);
				//MsgDeserialize
				if (success) lColumns.Add(IQLMsgDeserialize.ToString()); else lColumns.Add(Str.Empty);
				//QueryProcessorParse
				if (success) lColumns.Add(IQLQueryProcessorParse.ToString()); else lColumns.Add(Str.Empty);
			}
			if (this._threadGroup.conf.outputIQLBrokering)
			{
				//Broker Engine
				lColumns.Add(IQLBrokerEngine);
				//Client Engine(s)
				lColumns.Add(string.Join(",", IQLBorkerClients.ToArray()));
				//MergeAttributes
				if (success && IQLBorkerClients.Count > 0) lColumns.Add(IQLMergeAttributes.ToString()); else lColumns.Add(Str.Empty);
			}
			if (this._threadGroup.conf.outputIQLThreadCount)
			{
				//TODO - threads missing tid - https://sinequa.atlassian.net/browse/ES-12232
				//Header threads
				if (success) lColumns.Add(IQLHeaderThreadCount.ToString()); else lColumns.Add(Str.Empty);
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
				if (success) lColumns.Add((IQLHeaderThreadCount + dIQLEngineThreadCount.Sum(x => x.Value)).ToString()); else lColumns.Add(Str.Empty);
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
