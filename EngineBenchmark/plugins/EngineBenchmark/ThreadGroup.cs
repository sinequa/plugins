using Sinequa.Common;
using Sinequa.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace Sinequa.Plugin
{
    public class ThreadGroup
	{
		private CmdConfigEngineBenchmark _conf;
		private EngineBenchmark _engineBenchmarkCommand;

		public CmdConfigEngineBenchmark conf
		{
			get
			{
				return _conf;
			}
		}

		public EngineBenchmark engineBenchmark
		{
			get
			{
				return _engineBenchmarkCommand;
			}
		}

		public bool configLoadError { get; private set; }
		public string name { get; private set; }
		public string sql { get; private set; }
		public CCFile paramCustomFile { get; private set; }
		public char fileSep { get; private set; }
		public ParameterStrategy paramStrategy { get; private set; }
		public bool addUserACLs { get; private set; }
		public int threadNumber { get; private set; }
		public int threadSleepMin { get; private set; }
		public int threadSleepMax { get; private set; }
		public long maxExecutionTime { get; private set; }
		public int maxIteration { get; private set; }

		private readonly object syncLock = new object();
		private bool _paramsLoaded = false;
		private List<Dictionary<string, string>> _parameters = new List<Dictionary<string, string>>();
		private int _paramIndex = 0;
		private int _userACLsIndex = 0;
		private Random _rand = new Random();
		private ConcurrentDictionary<int, ThreadGroupOutput> _dOUtput = new ConcurrentDictionary<int, ThreadGroupOutput>();
		private ListOf<CCEngine> _Engines = new ListOf<CCEngine>();
		private ConcurrentDictionary<CCPrincipal, string> _dUsersACL = new ConcurrentDictionary<CCPrincipal, string>();
		private IDocContext _ctxt = new IDocContext();

		public int nbIterration = 0;
		public Stopwatch stopWatch = new Stopwatch();

		//dictionaries to hold engines indexes from 
		//dictionary of engine => list indexes (SearchRWA / FullTextSearchRWA / ExecuteDBQuery / Fetching DBQuery)
		public ConcurrentDictionary<string, List<string>> dEngineIndexes = new ConcurrentDictionary<string, List<string>>();
		public List<(string index, string engine)> lSortedEngineIndex
        {
            get
            {
				if (dEngineIndexes == null) return new List<(string elem, string engine)>();
				return GetSortedEngineList(dEngineIndexes);
			}
        }
		//dictionary of engine => list distribitions (distribution)
		public ConcurrentDictionary<string, List<string>> dEngineDistributions = new ConcurrentDictionary<string, List<string>>();
		public List<(string distribution, string engine)> lSortedEngineDistribution
		{
			get
			{
				if (dEngineDistributions == null) return new List<(string elem, string engine)>();
				return GetSortedEngineList(dEngineDistributions);
			}
		}
		//dictionary of engine => list correlations (correlation)
		public ConcurrentDictionary<string, List<string>> dEngineCorrelations = new ConcurrentDictionary<string, List<string>>();
		public List<(string correlation, string engine)> lSortedEngineCorrelation
		{
			get
			{
				if (dEngineCorrelations == null) return new List<(string elem, string engine)>();
				return GetSortedEngineList(dEngineCorrelations);
			}
		}

		public List<ThreadGroupOutput> outputs
		{
			get
			{
				return _dOUtput.Values.OrderBy(x => x.id).ToList();
			}
		}

		public enum SimpleStatProperty
		{
			processingTime,
			cursorSize,
			cursorSizeMB,
			clientFromPool,
			clientToPool,
			rowFetchTime,
			matchingRowCount,
			postGroupByMatchingRowCount,
			totalQueryTime,
			readCursor,
			curosrNetworkAndDeserialization
		}

		public enum MultiStatProperty
        {
			IQLSearchRWA,
			IQLFullTextSearchRWA,
			IQLExecuteDBQuery,
			IQLFetchingDBQuery,
			IQLDistribution,
			IQLCorrelation
		}

		public enum StatType
		{
			min,
			max,
			avg,
			stddev
		}

		public ThreadGroup(string name, string sql, CCFile paramCustomFile, char fileSep, ParameterStrategy paramStrategy,
			bool usersACL, int threadNumber, int threadSleepMin, int threadSleepMax, int maxExecutionTime, int maxIteration)
		{
			this.name = name;
			this.sql = sql;
			this.paramCustomFile = paramCustomFile;
			this.fileSep = fileSep;
			this.paramStrategy = paramStrategy;
			this.addUserACLs = usersACL;
			this.threadNumber = threadNumber;
			this.threadSleepMin = threadSleepMin * 1000;    //seconds to milliseconds
			this.threadSleepMax = threadSleepMax * 1000;    //seconds to milliseconds
			this.maxExecutionTime = maxExecutionTime * 1000;    //seconds to milliseconds
			this.maxIteration = maxIteration;
			this._ctxt.Doc = new IDocImpl();
		}

		//used to return a sorted list of <elem> <engine> => <elem> can ba an index, a distribution or a correlation
		private List<(string elem, string engine)> GetSortedEngineList(ConcurrentDictionary<string, List<string>> dEngineIndexes)
		{
			List<(string elem, string engine)> l = new List<(string index, string engine)>();
			foreach (string engine in dEngineIndexes.Keys)
			{
				dEngineIndexes.TryGetValue(engine, out List<string> lIndexes);
				foreach (string index in lIndexes) l.Add((index, engine));
			}
			return l.OrderBy(x => x.elem).ThenBy(x => x.engine).ToList(); ;
		}


		public void LogConf()
		{
			Sys.Log($"Name = [{this.name}]");
			Sys.Log($"SQL = [{this.sql}]");
			Sys.Log($"Custom File = [{this.paramCustomFile.Name}]");
			Sys.Log($"File separator = [{this.fileSep}]");
			Sys.Log($"Parameter strategy : [{Enum.GetName(typeof(ParameterStrategy), this.paramStrategy)}]");
			Sys.Log($"User ACL = [{this.addUserACLs}]");
			Sys.Log($"Thread Number = [{this.threadNumber}]");
			Sys.Log($"Thread Sleep Min = [{this.threadSleepMin}] ms");
			Sys.Log($"Thread Sleep Max = [{this.threadSleepMax}] ms");
			Sys.Log($"Max Execution Time = [{this.maxExecutionTime}] ms");
			Sys.Log($"Max Iteration = [{this.maxIteration}]");
		}

		public bool Init(EngineBenchmark engineBenchmarkCommand, CmdConfigEngineBenchmark conf)
		{
			this._engineBenchmarkCommand = engineBenchmarkCommand;
			this._conf = conf;

			this.Reset();
			if (!LoadParameters())
			{
				configLoadError = true;
				return false;
			}
			if (!GetEnginesFromStrategy(_conf.engineStategy, _conf.lEngines))
			{
				configLoadError = true;
				return false;
			}
			if (addUserACLs)
			{
				if (!LoadUsersACLs(_conf.domain, _conf.lUsers))
				{
					configLoadError = true;
					return false;
				}
			}
			return true;
		}

		private void Reset()
		{
			this.configLoadError = false;
			this.nbIterration = 0;
			this.stopWatch.Stop();
			this.stopWatch.Reset();

			this._paramsLoaded = false;
			this._parameters.Clear();
			this._paramIndex = 0;
			this._userACLsIndex = 0;
			this._rand = new Random();
			this._dOUtput.Clear();
			this._Engines = new ListOf<CCEngine>();
			this._dUsersACL.Clear();
		}

		private bool LoadParameters()
		{
			if (_paramsLoaded) return true;

			ListStr lines = Fs.FileToList(paramCustomFile.File, true);
			int i = 0;
			List<string> headers = new List<string>();
			foreach (string line in lines)
			{
				int realLineNumber = i + 1;

				List<string> lineColumns = new List<string>();
				lineColumns = line.Split(new Char[] { fileSep }).ToList();

				if (line.Length == 0 || lineColumns.Count == 0)
				{
					Sys.LogWarning($"Line [{realLineNumber}] is empty in parameter file [{paramCustomFile.Name}], line is ignored");
					i++;
					continue;
				}

				if (i == 0)
				{
					int duplicates = lineColumns.GroupBy(x => x).Where(g => g.Count() > 1).Count();
					if (duplicates > 0)
					{
						Sys.LogError($"Duplicates headers in parameter file [{paramCustomFile.Name}]");
						return false;
					}
					headers = lineColumns;
				}
				else
				{
					if (lineColumns.Count != headers.Count)
					{
						Sys.LogError($"Line [{realLineNumber}] does not have the same number of columns as header in parameter file [{paramCustomFile.Name}]");
						return false;
					}

					Dictionary<string, string> d = new Dictionary<string, string>();
					int j = 0;
					foreach (string param in lineColumns)
					{
						if (String.IsNullOrEmpty(param))
						{
							Sys.LogWarning($"Line [{realLineNumber}] contains an empty parameter for column [{headers[j]}] in parameter file [{paramCustomFile.Name}]");
						}
						d.Add(headers[j], param);
						j++;
					}
					_parameters.Add(d);
				}
				i++;
			}

			_paramsLoaded = true;

			return true;
		}

		private bool GetEnginesFromStrategy(EngineStategy engineStategy, ListOf<CCEngine> lEngines)
		{
			if (engineStategy == EngineStategy.First_available)
			{
				foreach (CCEngine engine in lEngines)
				{
					if (EngineIsAlive(engine.Name))
					{
						_Engines.Add(engine);
						break;
					}
				}
			}
			if (engineStategy == EngineStategy.Random)
			{
				foreach (CCEngine engine in lEngines)
				{
					if (EngineIsAlive(engine.Name))
					{
						_Engines.Add(engine);
					}
				}
			}
			if (_Engines.Count == 0) return false;
			return true;
		}

		private bool LoadUsersACLs(CCDomain domain, ListStr lUsers)
		{
			foreach (string userNameOrId in lUsers)
			{
				if (!Toolbox.GetUserRightsAsSqlStr(userNameOrId, domain.Name, _conf.securitySyntax, out CCPrincipal user, out string userRights)) return false;
				if (!_dUsersACL.TryAdd(user, userRights)) return false;
			}
			return true;
		}

		private bool EngineIsAlive(string engineName)
		{
			EngineCustomStatus ECS = Toolbox.GetEngineStatus(engineName);
			if (ECS == null) return false;
			return ECS.IsAlive;
		}

		public string GetSQLQuery(out Dictionary<string, string> dParams)
		{
			string sqlWithParams = sql;
			dParams = GetNextParameters();

			foreach (string paramName in dParams.Keys)
			{
				string token = "$" + paramName + "$";
				dParams.TryGetValue(paramName, out string value);
				sqlWithParams = Str.Replace(sqlWithParams, token, value);
			}

			//add user ACL
			//add _user_fullname_ as a parameter in params
			if (addUserACLs)
			{
				KeyValuePair<CCPrincipal, string> userACLs = GetUserACLs();
				sqlWithParams = Str.ReplaceFirst(sqlWithParams, "where", "where " + userACLs.Value + " and ");
				dParams.Add("_user_fullname_", userACLs.Key.FullName);
			}
			//current thread group no ACL but at least one thread group is using ACLs. Add empty values to keep the CSV aligned with headers
			else if (!addUserACLs && conf.threadGroups.Any(x => x.Value.addUserACLs == true))
			{
				dParams.Add("_user_fullname_", Str.Empty);
			}

			//add internalquerylog
			if (_conf.outputIQL)
			{
				if (!sqlWithParams.Contains("internalquerylog"))
				{
					sqlWithParams = Str.Replace(sqlWithParams, "select", "select internalquerylog,");
				}
			}

			//evaluate value patterns
			sqlWithParams = IDocHelper.GetValuePattern(_ctxt, sqlWithParams);

			return sqlWithParams;
		}

		public string GetEngine()
		{
			//first engine available, next 0, 1
			int index = _rand.Next(0, _Engines.Count);
			return _Engines.Get(index).Name;
		}

		private KeyValuePair<CCPrincipal, string> GetUserACLs()
		{
			KeyValuePair<CCPrincipal, string> kvp = new KeyValuePair<CCPrincipal, string>();
			if (this.paramStrategy == ParameterStrategy.Ordered)
			{
				lock (syncLock)
				{
					kvp = _dUsersACL.ElementAt(_userACLsIndex);
					_userACLsIndex = _userACLsIndex < _dUsersACL.Count - 1 ? _userACLsIndex + 1 : 0;
				}
			}
			if (this.paramStrategy == ParameterStrategy.Random)
			{
				lock (syncLock)
				{
					int index = _rand.Next(0, _dUsersACL.Count);
					kvp = _dUsersACL.ElementAt(index);
				}
			}
			return kvp;
		}

		public int GetSleep()
		{
			int sleepTime = 0;
			lock (syncLock)
			{
				sleepTime = _rand.Next(threadSleepMin, threadSleepMax);
			}
			return sleepTime;
		}

		private Dictionary<string, string> GetNextParameters()
		{
			Dictionary<string, string> d = null;
			if (this.paramStrategy == ParameterStrategy.Ordered)
			{
				lock (syncLock)
				{
					d = new Dictionary<string, string>(_parameters.ElementAt(_paramIndex));
					_paramIndex = _paramIndex < _parameters.Count - 1 ? _paramIndex + 1 : 0;
				}
			}
			if (this.paramStrategy == ParameterStrategy.Random)
			{
				lock (syncLock)
				{
					int index = _rand.Next(0, _parameters.Count);
					d = new Dictionary<string, string>(_parameters.ElementAt(index));
				}
			}
			return d;
		}

		public bool AddOutput(int id, int threadId, DateTime start, DateTime end, out ThreadGroupOutput tGroupOUtput)
		{
			tGroupOUtput = new ThreadGroupOutput(this, id, threadId, start, end);
			if (!_dOUtput.TryAdd(id, tGroupOUtput))
			{
				tGroupOUtput = null;
				return false;
			}
			return true;
		}

		public void WriteQueryOutpout()
        {
			Stopwatch swWriteFile = new Stopwatch();

			string outputQueryFilePath = Toolbox.GetOutputFilePath(conf.outputFolderPath, $"{name}_Queries", "csv");
			swWriteFile.Start();
			bool bHeaders = false;

			using (StreamWriter sw = new StreamWriter(outputQueryFilePath, false, Encoding.UTF8))
			{
				foreach (ThreadGroupOutput tGoupOutout in outputs)
				{
					if (!bHeaders)
					{
						sw.WriteLine(tGoupOutout.QueryOutputCSVHeader(conf.outputCSVSeparator));
						bHeaders = true;
					}
					sw.WriteLine(tGoupOutout.QueryOutputCSVRow(conf.outputCSVSeparator));
				}	
			}

			swWriteFile.Stop();
			Sys.Log($"Create Queries output file [{outputQueryFilePath}] [{Sys.TimerGetText(swWriteFile.ElapsedMilliseconds)}]");
			swWriteFile.Reset();
		}

		public void WriteCursorSizeOutput()
        {
			Stopwatch swWriteFile = new Stopwatch();

			string outputCursorSizeFilePath = Toolbox.GetOutputFilePath(conf.outputFolderPath, $"{name}_CursorSize", "csv");
			swWriteFile.Start();
			bool bHeaders = false;

			using (StreamWriter sw = new StreamWriter(outputCursorSizeFilePath, false, Encoding.UTF8))
			{
				foreach (ThreadGroupOutput tGoupOutout in outputs)
				{
					if (!bHeaders)
					{
						sw.WriteLine(tGoupOutout.CursorSizeCSVHeader(conf.outputCSVSeparator));
						bHeaders = true;
					}
					sw.WriteLine(tGoupOutout.CursorSizeCSVRow(conf.outputCSVSeparator));
				}
			}

			swWriteFile.Stop();
			Sys.Log($"Create Cursor Size output file [{outputCursorSizeFilePath}] [{Sys.TimerGetText(swWriteFile.ElapsedMilliseconds)}]");
			swWriteFile.Reset();
		}

		public List<ThreadGroupOutput> GetOutputByStatus(bool success)
		{
			return _dOUtput.Where(x => x.Value.success == success).Select(x => x.Value).ToList();			
		}

        #region stats

		private string _statsFormatPercent = "N2";
		private string _statsFormatMs = "N2";
		private string _statsFormatMB = "N3";
		private string _statsFormatCount = "N0";
		private string _statsFormatQPS = "N2";

		private List<double> lPercentil = new List<double>() { 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };

		public double GetOutputSimpleStat(SimpleStatProperty p, StatType t, bool status)
		{
			List<ThreadGroupOutput> l = GetOutputByStatus(status);
			if (l.Count == 0) return -1;

			if (p == SimpleStatProperty.processingTime && t == StatType.min) return l.Min(x => x.processingTime);
			if (p == SimpleStatProperty.processingTime && t == StatType.max) return l.Max(x => x.processingTime);
			if (p == SimpleStatProperty.processingTime && t == StatType.avg) return l.Average(x => x.processingTime);
			if (p == SimpleStatProperty.processingTime && t == StatType.stddev) return StdDev(l.Select(x => x.processingTime));

			if (p == SimpleStatProperty.cursorSize && t == StatType.min) return l.Min(x => x.cursorSize);
			if (p == SimpleStatProperty.cursorSize && t == StatType.max) return l.Max(x => x.cursorSize);
			if (p == SimpleStatProperty.cursorSize && t == StatType.avg) return l.Average(x => x.cursorSize);
			if (p == SimpleStatProperty.cursorSize && t == StatType.stddev) return StdDev(l.Select(x => x.cursorSize));

			if (p == SimpleStatProperty.cursorSizeMB && t == StatType.min) return l.Min(x => x.cursorSizeMB);
			if (p == SimpleStatProperty.cursorSizeMB && t == StatType.max) return l.Max(x => x.cursorSizeMB);
			if (p == SimpleStatProperty.cursorSizeMB && t == StatType.avg) return l.Average(x => x.cursorSizeMB);
			if (p == SimpleStatProperty.cursorSizeMB && t == StatType.stddev) return StdDev(l.Select(x => x.cursorSizeMB));

			if (p == SimpleStatProperty.clientFromPool && t == StatType.min) return l.Min(x => x.clientFromPool);
			if (p == SimpleStatProperty.clientFromPool && t == StatType.max) return l.Max(x => x.clientFromPool);
			if (p == SimpleStatProperty.clientFromPool && t == StatType.avg) return l.Average(x => x.clientFromPool);
			if (p == SimpleStatProperty.clientFromPool && t == StatType.stddev) return StdDev(l.Select(x => x.clientFromPool));

			if (p == SimpleStatProperty.clientToPool && t == StatType.min) return l.Min(x => x.clientToPool);
			if (p == SimpleStatProperty.clientToPool && t == StatType.max) return l.Max(x => x.clientToPool);
			if (p == SimpleStatProperty.clientToPool && t == StatType.avg) return l.Average(x => x.clientToPool);
			if (p == SimpleStatProperty.clientToPool && t == StatType.stddev) return StdDev(l.Select(x => x.clientToPool));

			if (p == SimpleStatProperty.rowFetchTime && t == StatType.min) return l.Min(x => x.rowFetchTime);
			if (p == SimpleStatProperty.rowFetchTime && t == StatType.max) return l.Max(x => x.rowFetchTime);
			if (p == SimpleStatProperty.rowFetchTime && t == StatType.avg) return l.Average(x => x.rowFetchTime);
			if (p == SimpleStatProperty.rowFetchTime && t == StatType.stddev) return StdDev(l.Select(x => x.rowFetchTime));

			if (p == SimpleStatProperty.matchingRowCount && t == StatType.min) return l.Min(x => x.matchingRowCount);
			if (p == SimpleStatProperty.matchingRowCount && t == StatType.max) return l.Max(x => x.matchingRowCount);
			if (p == SimpleStatProperty.matchingRowCount && t == StatType.avg) return l.Average(x => x.matchingRowCount);
			if (p == SimpleStatProperty.matchingRowCount && t == StatType.stddev) return StdDev(l.Select(x => x.matchingRowCount));

			if (p == SimpleStatProperty.postGroupByMatchingRowCount && t == StatType.min) return l.Min(x => x.postGroupByMatchingRowCount);
			if (p == SimpleStatProperty.postGroupByMatchingRowCount && t == StatType.max) return l.Max(x => x.postGroupByMatchingRowCount);
			if (p == SimpleStatProperty.postGroupByMatchingRowCount && t == StatType.avg) return l.Average(x => x.postGroupByMatchingRowCount);
			if (p == SimpleStatProperty.postGroupByMatchingRowCount && t == StatType.stddev) return StdDev(l.Select(x => x.postGroupByMatchingRowCount));

			if (p == SimpleStatProperty.totalQueryTime && t == StatType.min) return l.Min(x => x.totalQueryTime);
			if (p == SimpleStatProperty.totalQueryTime && t == StatType.max) return l.Max(x => x.totalQueryTime);
			if (p == SimpleStatProperty.totalQueryTime && t == StatType.avg) return l.Average(x => x.totalQueryTime);
			if (p == SimpleStatProperty.totalQueryTime && t == StatType.stddev) return StdDev(l.Select(x => x.totalQueryTime));

			if (p == SimpleStatProperty.readCursor && t == StatType.min) return l.Min(x => x.readCursor);
			if (p == SimpleStatProperty.readCursor && t == StatType.max) return l.Max(x => x.readCursor);
			if (p == SimpleStatProperty.readCursor && t == StatType.avg) return l.Average(x => x.readCursor);
			if (p == SimpleStatProperty.readCursor && t == StatType.stddev) return StdDev(l.Select(x => x.readCursor));

			if (p == SimpleStatProperty.curosrNetworkAndDeserialization && t == StatType.min) return l.Min(x => x.curosrNetworkAndDeserialization);
			if (p == SimpleStatProperty.curosrNetworkAndDeserialization && t == StatType.max) return l.Max(x => x.curosrNetworkAndDeserialization);
			if (p == SimpleStatProperty.curosrNetworkAndDeserialization && t == StatType.avg) return l.Average(x => x.curosrNetworkAndDeserialization);
			if (p == SimpleStatProperty.curosrNetworkAndDeserialization && t == StatType.stddev) return StdDev(l.Select(x => x.curosrNetworkAndDeserialization));

			return -1;
		}

		//elem can be: index, distribution or correlation
		public double GetOutputMultiStat(MultiStatProperty p, StatType t, bool status, string engineName, string elem)
		{
			List<ThreadGroupOutput> l = GetOutputByStatus(status);
			if (l.Count == 0) return -1;

			List<(string engine, string elem, double duration)> lAggregate = new List<(string engine, string elem, double duration)>(); 
			if (p == MultiStatProperty.IQLSearchRWA) lAggregate = l.Select(x => x.IQLSearchRWA).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLFullTextSearchRWA) lAggregate = l.Select(x => x.IQLFullTextSearchRWA).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLExecuteDBQuery) lAggregate = l.Select(x => x.IQLExecuteDBQuery).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLFetchingDBQuery) lAggregate = l.Select(x => x.IQLFetchingDBQuery).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLDistribution) lAggregate = l.Select(x => x.IQLDistribution).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLCorrelation) lAggregate = l.Select(x => x.IQLCorrelation).Aggregate((x, y) => x.Union(y).ToList()).ToList();

			if (lAggregate.Count == 0) return -1;
			List<(string engine, string elem, double duration)> lFiltered = lAggregate.Where(x => Str.EQNC(x.engine, engineName) && Str.EQNC(x.elem, elem)).ToList();
			if (lFiltered.Count == 0) return -1;

			if (t == StatType.min) return lFiltered.Min(x => x.duration);
			if (t == StatType.max) return lFiltered.Max(x => x.duration);
			if (t == StatType.avg) return lFiltered.Average(x => x.duration);
			if (t == StatType.stddev) return StdDev(lFiltered.Select(x => x.duration));

			return -1;
		}
		
		private double StdDev(IEnumerable<double> values)
        {
			double ret = 0;
			int count = values.Count();
			if (count > 1)
			{
				//Compute the Average
				double avg = values.Average();

				//Perform the Sum of (value-avg)^2
				double sum = values.Sum(d => (d - avg) * (d - avg));

				//Put it all together
				ret = Math.Sqrt(sum / count);
			}
			return ret;
		}

		public double GetOutputSimpleStatPercentile(SimpleStatProperty p, double percentil)
		{
			List<ThreadGroupOutput> l = GetOutputByStatus(true);
			if (l.Count == 0) return -1;
			
			List<double> lValues = new List<double>();
			double maxValue = -1;
			switch (p)
			{
				case SimpleStatProperty.processingTime:
					lValues = l.OrderBy(x => x.processingTime).Select(x => x.processingTime).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.cursorSize:
					lValues = l.OrderBy(x => x.cursorSize).Select(x => x.cursorSize).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.cursorSizeMB:
					lValues = l.OrderBy(x => x.cursorSizeMB).Select(x => x.cursorSizeMB).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.clientFromPool:
					lValues = l.OrderBy(x => x.clientFromPool).Select(x => x.clientFromPool).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.clientToPool:
					lValues = l.OrderBy(x => x.clientToPool).Select(x => x.clientToPool).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.rowFetchTime:
					lValues = l.OrderBy(x => x.rowFetchTime).Select(x => x.rowFetchTime).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.matchingRowCount:
					lValues = l.OrderBy(x => x.matchingRowCount).Select(x => x.matchingRowCount).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.postGroupByMatchingRowCount:
					lValues = l.OrderBy(x => x.postGroupByMatchingRowCount).Select(x => x.postGroupByMatchingRowCount).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.totalQueryTime:
					lValues = l.OrderBy(x => x.totalQueryTime).Select(x => x.totalQueryTime).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.readCursor:
					lValues = l.OrderBy(x => x.readCursor).Select(x => x.readCursor).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				case SimpleStatProperty.curosrNetworkAndDeserialization:
					lValues = l.OrderBy(x => x.curosrNetworkAndDeserialization).Select(x => x.curosrNetworkAndDeserialization).ToList();
					maxValue = GetOutputSimpleStat(p, StatType.max, true);
					break;
				default: return -1;
			}

			int index = (int)Math.Ceiling(percentil * lValues.Count);
			if (index >= lValues.Count) return maxValue;
			return lValues.ElementAt(index);
		}

		public double GetOutputMultiStatPercentile(MultiStatProperty p, double percentil, string engineName, string elem)
		{
			List<ThreadGroupOutput> l = GetOutputByStatus(true);
			if (l.Count == 0) return -1;

			List<(string engine, string elem, double duration)> lAggregate = new List<(string engine, string elem, double duration)>();
			if (p == MultiStatProperty.IQLSearchRWA) lAggregate = l.Select(x => x.IQLSearchRWA).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLFullTextSearchRWA) lAggregate = l.Select(x => x.IQLFullTextSearchRWA).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLExecuteDBQuery) lAggregate = l.Select(x => x.IQLExecuteDBQuery).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLFetchingDBQuery) lAggregate = l.Select(x => x.IQLFetchingDBQuery).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLDistribution) lAggregate = l.Select(x => x.IQLDistribution).Aggregate((x, y) => x.Union(y).ToList()).ToList();
			if (p == MultiStatProperty.IQLCorrelation) lAggregate = l.Select(x => x.IQLCorrelation).Aggregate((x, y) => x.Union(y).ToList()).ToList();

			if (lAggregate.Count == 0) return -1;
			List<(string engine, string elem, double duration)> lFiltered = lAggregate.Where(x => Str.EQNC(x.engine, engineName) && Str.EQNC(x.elem, elem)).ToList();
			if (lFiltered.Count == 0) return -1;
			List<double> lValues = lFiltered.OrderBy(x => x.duration).Select(x => x.duration).ToList();

			double maxValue = GetOutputMultiStat(p, StatType.max, true, engineName, elem);

			int index = (int)Math.Ceiling(percentil * lValues.Count);
			if (index >= lValues.Count) return maxValue;
			return lValues.ElementAt(index);
		}

		public double GetQPS()
		{
			List<ThreadGroupOutput> l = GetOutputByStatus(true);
			return ((double)(GetOutputByStatus(true).Count) / ((double)(stopWatch.ElapsedMilliseconds) / 1000));
		}

		public double GetPercentageSuccessQueries()
        {
			return ((GetOutputByStatus(true).Count() * 100) / _dOUtput.Count());
        }

		public double GetPercentageFailedQueries()
        {
			return ((GetOutputByStatus(false).Count() * 100) / _dOUtput.Count());
		}

		public List<(string row, string column, string value)> GetTableRowMultiStat(MultiStatProperty property, string engine, string elem, string format, bool min = true, bool avg = true, bool max = true, bool stdDev = true, bool percentil = true)
        {
			List<(string row, string column, string value)> tableRow = new List<(string row, string column, string value)>();
			string propertyName = $"[{elem}] [{engine}]";
			if (min)
			{
				double d = GetOutputMultiStat(property, StatType.min, true, engine, elem);
				tableRow.Add((propertyName, "MIN", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (avg)
			{
				double d = GetOutputMultiStat(property, StatType.avg, true, engine, elem);
				tableRow.Add((propertyName, "AVG", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (max)
			{
				double d = GetOutputMultiStat(property, StatType.max, true, engine, elem);
				tableRow.Add((propertyName, "MAX", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (stdDev)
			{
				double d = GetOutputMultiStat(property, StatType.stddev, true, engine, elem);
				tableRow.Add((propertyName, "StdDev", d == -1 ? Str.Empty : d.ToString(format)));
			}
			
			if (percentil)
			{
				foreach (double dPercentil in lPercentil)
				{
					double d = GetOutputMultiStatPercentile(property, dPercentil, engine, elem);
					tableRow.Add((propertyName, $"{dPercentil * 100}th", d == -1 ? Str.Empty : d.ToString(format)));
				}
			}
			
			return tableRow;
		}

		public List<(string row, string column, string value)> GetTableRowSimpleStat(SimpleStatProperty property, string format, bool min = true, bool avg = true, bool max = true, bool stdDev = true, bool percentil = true)
        {
			List<(string row, string column, string value)> tableRow = new List<(string row, string column, string value)>();
			string propertyName = Enum.GetName(typeof(SimpleStatProperty), property);
			if (min)
			{
				double d = GetOutputSimpleStat(property, StatType.min, true); 
				tableRow.Add((propertyName, "MIN", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (avg)
			{
				double d = GetOutputSimpleStat(property, StatType.avg, true);
				tableRow.Add((propertyName, "AVG", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (max)
			{
				double d = GetOutputSimpleStat(property, StatType.max, true);
				tableRow.Add((propertyName, "MAX", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (stdDev)
			{
				double d = GetOutputSimpleStat(property, StatType.stddev, true);
				tableRow.Add((propertyName, "StdDev", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (percentil)
			{
				foreach(double dPercentil in lPercentil)
                {
					double d = GetOutputSimpleStatPercentile(property, dPercentil);
					tableRow.Add((propertyName, $"{dPercentil * 100}th", d == -1 ? Str.Empty : d.ToString(format)));
				}
			}
			return tableRow;
		}

		public void LogQueriesStats()
		{
			Stopwatch swStats = new Stopwatch();
			swStats.Start();

			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Thread Group [{name}] - Execution time [{Sys.TimerGetText(stopWatch.ElapsedMilliseconds)}]");
			Sys.Log($"Thread Group [{name}] - Number of iterations [{_dOUtput.Count()}]");
			Sys.Log($"Thread Group [{name}] - Number of success queries [{GetOutputByStatus(true).Count()}/{_dOUtput.Count()}] [{GetPercentageSuccessQueries().ToString(_statsFormatPercent)}%]");
			Sys.Log($"Thread Group [{name}] - Number of failed queries [{GetOutputByStatus(false).Count()}/{_dOUtput.Count()}] [{GetPercentageFailedQueries().ToString(_statsFormatPercent)}%]");
			Sys.Log($"Thread Group [{name}] - QPS (Query Per Second) [{GetQPS().ToString(_statsFormatQPS)}] ");

			if (conf.outputQueryTimers)
			{
				List<(string row, string column, string value)> lQueryTimers = new List<(string row, string column, string value)>();
				//totalQueryTime
				lQueryTimers.Add(("totalQueryTime", "unit", "ms"));
				lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.totalQueryTime, _statsFormatMs)).ToList();
				//processingTime
				lQueryTimers.Add(("processingTime", "unit", "ms"));
				lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.processingTime, _statsFormatMs)).ToList();
				//rowFetchTime
				lQueryTimers.Add(("rowFetchTime", "unit", "ms"));
				lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.rowFetchTime, _statsFormatMs)).ToList();
				//readCursor
				lQueryTimers.Add(("readCursor", "unit", "ms"));
				lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.readCursor, _statsFormatMs)).ToList();
				//cursorSizeMB
				lQueryTimers.Add(("cursorSizeMB", "unit", "MB"));
				lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.cursorSizeMB, _statsFormatMB)).ToList();

				if (conf.outputQueryInfo)
                {
					//matchingRowCount
					lQueryTimers.Add(("matchingRowCount", "unit", "rows"));
					lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.matchingRowCount, _statsFormatCount)).ToList();
					//postGroupByMatchingRowCount
					lQueryTimers.Add(("postGroupByMatchingRowCount", "unit", "rows"));
					lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.postGroupByMatchingRowCount, _statsFormatCount)).ToList();
				}
                if (conf.outputClientTimers)
                {
					//clientFromPool
					lQueryTimers.Add(("clientFromPool", "unit", "ms"));
					lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.clientFromPool, _statsFormatMs)).ToList();
					//clientToPool
					lQueryTimers.Add(("clientToPool", "unit", "ms"));
					lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.clientToPool, _statsFormatMs)).ToList();
				}
                if (conf.outputCurosrNetworkAndDeserializationTimer)
                {
					//curosrNetworkAndDeserialization
					lQueryTimers.Add(("curosrNetworkAndDeserialization", "unit", "ms"));
					lQueryTimers = lQueryTimers.Union(GetTableRowSimpleStat(SimpleStatProperty.curosrNetworkAndDeserialization, _statsFormatMs)).ToList();
				}
				LogArray LAQueryTimers = new LogArray(lQueryTimers, $"[{name}]");
				LAQueryTimers.Log();
			}
			
			if (conf.outputIQLSearchRWA)
			{
				//SearchRWA
				List<(string row, string column, string value)> lQueryIQLSearchRWATimers = new List<(string row, string column, string value)>();
				//FullTextSearchRWA
				List<(string row, string column, string value)> lQueryIQLFullTextSearchRWATimers = new List<(string row, string column, string value)>();
				foreach ((string index, string engine) x in lSortedEngineIndex)
				{
					//SearchRWA
					lQueryIQLSearchRWATimers.Add(($"[{x.index}] [{x.engine}]", "unit", "ms"));
					lQueryIQLSearchRWATimers = lQueryIQLSearchRWATimers.Union(GetTableRowMultiStat(MultiStatProperty.IQLSearchRWA, x.engine, x.index, _statsFormatMs)).ToList();
					//FullTextSearchRWA
					lQueryIQLFullTextSearchRWATimers.Add(($"[{x.index}] [{x.engine}]", "unit", "ms"));
					lQueryIQLFullTextSearchRWATimers = lQueryIQLFullTextSearchRWATimers.Union(GetTableRowMultiStat(MultiStatProperty.IQLFullTextSearchRWA, x.engine, x.index, _statsFormatMs)).ToList();
				}
				//SearchRWA
				LogArray LAQueryIQLSearchRWATimers = new LogArray(lQueryIQLSearchRWATimers, $"[{name}] SearchRWA");
				LAQueryIQLSearchRWATimers.Log();
				//FullTextSearchRWA
				LogArray LAQueryIQLFullTextSearchRWATimers = new LogArray(lQueryIQLFullTextSearchRWATimers, $"[{name}] FullTextSearchRWA");
				LAQueryIQLFullTextSearchRWATimers.Log();
			}

			if (conf.outputIQLDBQuery)
			{
				//ExecuteDBQuery
				List<(string row, string column, string value)> lQueryIQLExecuteDBQueryTimers = new List<(string row, string column, string value)>();
				//FetchingDBQuery
				List<(string row, string column, string value)> lQueryIQLFetchingDBQueryTimers = new List<(string row, string column, string value)>();
				foreach ((string index, string engine) x in lSortedEngineIndex)
				{
					//ExecuteDBQuery
					lQueryIQLExecuteDBQueryTimers.Add(($"[{x.index}] [{x.engine}]", "unit", "ms"));
					lQueryIQLExecuteDBQueryTimers = lQueryIQLExecuteDBQueryTimers.Union(GetTableRowMultiStat(MultiStatProperty.IQLExecuteDBQuery, x.engine, x.index, _statsFormatMs)).ToList();
					//FetchingDBQuery
					lQueryIQLFetchingDBQueryTimers.Add(($"[{x.index}] [{x.engine}]", "unit", "ms"));
					lQueryIQLFetchingDBQueryTimers = lQueryIQLFetchingDBQueryTimers.Union(GetTableRowMultiStat(MultiStatProperty.IQLFetchingDBQuery, x.engine, x.index, _statsFormatMs)).ToList();
				}
				//ExecuteDBQuery
				LogArray LAQueryIQLExecuteDBQueryTimers = new LogArray(lQueryIQLExecuteDBQueryTimers, $"[{name}] ExecuteDBQuery");
				LAQueryIQLExecuteDBQueryTimers.Log();
				//FetchingDBQuery
				LogArray LAQueryIQLFetchingDBQueryTimers = new LogArray(lQueryIQLFetchingDBQueryTimers, $"[{name}] FetchingDBQuery");
				LAQueryIQLFetchingDBQueryTimers.Log();
			}

			if (conf.outputIQLDistributionsCorrelations)
			{
				//Distribution
				List<(string row, string column, string value)> lQueryIQLDistributionTimers = new List<(string row, string column, string value)>();
				foreach ((string dist, string engine) x in lSortedEngineDistribution)
				{
					lQueryIQLDistributionTimers.Add(($"[{x.dist}] [{x.engine}]", "unit", "ms"));
					lQueryIQLDistributionTimers = lQueryIQLDistributionTimers.Union(GetTableRowMultiStat(MultiStatProperty.IQLDistribution, x.engine, x.dist, _statsFormatMs)).ToList();
				}
				LogArray LAQueryIQLDistributionTimers = new LogArray(lQueryIQLDistributionTimers, $"[{name}] Distribution");
				LAQueryIQLDistributionTimers.Log();
				

				//Correlation
				List<(string row, string column, string value)> lQueryIQLCorrelationTimers = new List<(string row, string column, string value)>();
				foreach ((string correl, string engine) x in lSortedEngineCorrelation)
				{
					lQueryIQLCorrelationTimers.Add(($"[{x.correl}] [{x.engine}]", "unit", "ms"));
					lQueryIQLCorrelationTimers = lQueryIQLCorrelationTimers.Union(GetTableRowMultiStat(MultiStatProperty.IQLCorrelation, x.engine, x.correl, _statsFormatMs)).ToList();
				}
				LogArray LAQueryIQLCorrelationTimers = new LogArray(lQueryIQLCorrelationTimers, $"[{name}] Correlation");
				LAQueryIQLCorrelationTimers.Log();
			}

			swStats.Stop();
			Sys.Log($"Thread Group [{name}] - Compute stats [{Sys.TimerGetText(swStats.ElapsedMilliseconds)}]");
		}

        #endregion
    }

}
