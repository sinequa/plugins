using Sinequa.Common;
using Sinequa.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

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
		private ConcurrentDictionary<int, ThreadGroupOutput> _dOutput = new ConcurrentDictionary<int, ThreadGroupOutput>();

		private Stopwatch _stopWatch = new Stopwatch();
		public long ExecutionTime
		{
			get { return _stopWatch.ElapsedMilliseconds; }
		}
		private int _iterations = 0;
		public int iterations
		{
			get { return _iterations; }
		}
		public int IncrementIterations()
		{
			return Interlocked.Increment(ref this._iterations);
		}

		public int outputCount
		{
			get { return _dOutput.Count; }
		}
		private ListOf<CCEngine> _Engines = new ListOf<CCEngine>();
		private ConcurrentDictionary<CCPrincipal, string> _dUsersACL = new ConcurrentDictionary<CCPrincipal, string>();
		private IDocContext _ctxt = new IDocContext();

		#region state

		private ThreadGroupState _state;
		public ThreadGroupState state
		{
			get { return _state; }
			set
			{
				lock (syncLock)
				{
					_state = value;
				}
			}
		}

		public enum ThreadGroupState
		{
			none,           //not initialized yet
			error,          //init error
			waiting,        //waiting to be executed
			running,        //execution is running
			done            //execution is over
		}

		#endregion

		//store InternalQueryLog data (engines, indexes, distributions, correlations)
		#region InternalQueryLog dictionnaries

		//dictionnary of distribution or correlation aliases. Used to resolve multiple distributions / correlations on same column
		//key => distribution(...)
		//value => count_MatchingPartnames
		public Dictionary<string, string> dDistCorrelAliases { get; private set; }

		//dictionaries to hold engines indexes from 
		//dictionary of engine => list indexes (SearchRWA / FullTextSearchRWA / ExecuteDBQuery / Fetching DBQuery)
		public ConcurrentDictionary<string, List<string>> dEngineIndexes;
		//dictionary of engine => list distribitions (distribution)
		public ConcurrentDictionary<string, List<string>> dEngineDistributions;
		//dictionary of engine => list correlations (correlation)
		public ConcurrentDictionary<string, List<string>> dEngineCorrelations;

		#endregion

		//cache to optimize stats generation
		private Dictionary<MultiStatProperty, List<(string engine, string elem, double duration)>> _multiStatPropertyCache = new Dictionary<MultiStatProperty, List<(string engine, string elem, double duration)>>();

		public List<(string index, string engine)> lSortedEngineIndex
		{
			get
			{
				if (dEngineIndexes == null) return new List<(string elem, string engine)>();
				return GetSortedEngineList(dEngineIndexes);
			}
		}

		public List<(string distribution, string engine)> lSortedEngineDistribution
		{
			get
			{
				if (dEngineDistributions == null) return new List<(string elem, string engine)>();
				return GetSortedEngineList(dEngineDistributions);
			}
		}

		public List<(string correlation, string engine)> lSortedEngineCorrelation
		{
			get
			{
				if (dEngineCorrelations == null) return new List<(string elem, string engine)>();
				return GetSortedEngineList(dEngineCorrelations);
			}
		}

		public List<ThreadGroupOutput> sortedOutputs
		{
			get
			{
				return _dOutput.Values.OrderBy(x => x.iteration).ToList();
			}
		}

		//enum used for stats generation
		#region stats enum

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
			curosrNetworkAndDeserialization,
			RFMBoostExact,
			RFMBoostSimilar,
		}

		public enum MultiStatProperty
		{
			IQLSearchRWA,
			IQLFullTextSearchRWA,
			IQLExecuteDBQuery,
			IQLFetchingDBQuery,
			IQLDistribution,
			IQLCorrelation,
			IQLAcqRLk
		}

		public enum StatType
		{
			min,
			max,
			avg,
			stddev
		}

		#endregion

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
			this.maxExecutionTime = maxExecutionTime == -1 ? maxExecutionTime : maxExecutionTime * 1000;    //seconds to milliseconds
			this.maxIteration = maxIteration;
			this.state = ThreadGroupState.none;
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
			l = l.OrderBy(x => x.elem).ThenBy(x => x.engine).ToList(); ;
			return l;
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
			if (this.maxExecutionTime == -1) Sys.Log($"Max Execution Time = [{this.maxExecutionTime}] (infinite)");
			else Sys.Log($"Max Execution Time = [{this.maxExecutionTime}] ms");
			if (this.maxIteration == -1) Sys.Log($"Max Iteration = [{this.maxIteration}] (infinite)");
			else Sys.Log($"Max Iteration = [{this.maxIteration}]");
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
				state = ThreadGroupState.error;
				return false;
			}
			if (addUserACLs)
			{
				if (!LoadUsersACLs(_conf.domain, _conf.lUsers))
				{
					configLoadError = true;
					state = ThreadGroupState.error;
					return false;
				}
			}
			if (!GetDistributionsCorrelationsFromQuery())
			{
				configLoadError = true;
				state = ThreadGroupState.error;
				return false;
			}

			state = ThreadGroupState.waiting;
			return true;
		}

		private void Reset()
		{
			this.configLoadError = false;
			this._iterations = 0;
			this._stopWatch.Stop();
			this._stopWatch.Reset();
			this.state = ThreadGroupState.none;

			this.dEngineIndexes = new ConcurrentDictionary<string, List<string>>();
			this.dEngineDistributions = new ConcurrentDictionary<string, List<string>>();
			this.dEngineCorrelations = new ConcurrentDictionary<string, List<string>>();
			this.dDistCorrelAliases = new Dictionary<string, string>();

			this._paramsLoaded = false;
			this._parameters.Clear();
			this._paramIndex = 0;
			this._userACLsIndex = 0;
			this._rand = new Random();
			this._dOutput.Clear();
			this._Engines = new ListOf<CCEngine>();
			this._dUsersACL.Clear();
		}

		public void Start()
		{
			_stopWatch.Start();
			state = ThreadGroupState.running;
		}

		public void Stop()
		{
			_stopWatch.Stop();
			state = ThreadGroupState.done;
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

			Sys.Log($"Thread Group [{this.name}] parameter file [{paramCustomFile.Name}] params [{string.Join(fileSep.ToString(), headers)}] lines [{_parameters.Count()}]");

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

		private bool GetDistributionsCorrelationsFromQuery()
		{
			string pattern = @"(distribution|correlation)\s*\(\s*\'(.*?)\'\s*\)\s*as\s*(.*?)[,|\s]";
			//group1 => distribution|correlation
			//group2 => options => options btw (' and ')
			//group3 => alias

			RegexOptions options = RegexOptions.Singleline;

			if (_conf.outputIQLDistributionsCorrelations)
			{
				string select = Str.ParseFromTo(sql, $"SELECT ", $" FROM ", true);
				if (string.IsNullOrEmpty(select))
				{
					Sys.LogError($"Cannot parse SQL expression. SELECT ... FROM ... not detected");
					return false;
				}
				select += " ";  //add space at the end to ensure regex match

				foreach (Match match in Regex.Matches(select, pattern, options))
				{
					if (match.Groups.Count == 4 && !String.IsNullOrEmpty(match.Groups[1].Value) && !String.IsNullOrEmpty(match.Groups[2].Value) && !String.IsNullOrEmpty(match.Groups[3].Value))
					{
						string type = match.Groups[1].Value;
						//clean exp to match InternalQueryLog format: distribution|correlation(options)
						string opts = Str.Replace(match.Groups[2].Value, " ", "");
						string alias = match.Groups[3].Value;

						string exp = $"{type}({opts})";

						dDistCorrelAliases.Add(exp, alias);
					}
					else
					{
						Sys.LogError($"Cannot parse expression [{match.Value}] in [{select}]");
						return false;
					}
				}
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

			//parameters / variables
			ParameterSet paramSet = new ParameterSet();
			foreach (string paramName in dParams.Keys)
			{
				dParams.TryGetValue(paramName, out string value);
				paramSet.SetValue(paramName, value);
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
			if (_conf.outputIQL || _conf.dumpIQL)
			{
				if (!sqlWithParams.Contains("internalquerylog"))
				{
					sqlWithParams = Str.Replace(sqlWithParams, "select", "select internalquerylog,");
				}
			}
			//add internalqueryanalysis
			if (_conf.dumpIQA)
            {
				if (!sqlWithParams.Contains("internalqueryanalysis"))
				{
					sqlWithParams = Str.Replace(sqlWithParams, "select", "select internalqueryanalysis,");
				}
			}

			//evaluate value patterns
			sqlWithParams = IDocHelper.GetValuePattern(_ctxt, paramSet, sqlWithParams);

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
			if (!_dOutput.TryAdd(id, tGroupOUtput))
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

			Sys.Log2(200, $"WriteQueryOutpout [{name}]");

			using (StreamWriter sw = new StreamWriter(outputQueryFilePath, false, Encoding.UTF8))
			{
				string header = Str.Empty;
				string row = Str.Empty;
				foreach (ThreadGroupOutput tGoupOutout in sortedOutputs)
				{
					if (!bHeaders)
					{
						header = tGoupOutout.QueryOutputCSVHeader(conf.outputCSVSeparator);
						Sys.Log2(200, $"WriteQueryOutpout [{name}] header [{header}]");
						sw.WriteLine(header);
						bHeaders = true;
					}
					row = tGoupOutout.QueryOutputCSVRow(conf.outputCSVSeparator);
					Sys.Log2(200, $"WriteQueryOutpout [{name}] row [{row}]");
					sw.WriteLine(row);
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

			Sys.Log2(200, $"WriteCursorSizeOutput [{name}]");

			using (StreamWriter sw = new StreamWriter(outputCursorSizeFilePath, false, Encoding.UTF8))
			{
				string header = Str.Empty;
				string row = Str.Empty;
				foreach (ThreadGroupOutput tGoupOutout in sortedOutputs)
				{
					if (!bHeaders)
					{
						header = tGoupOutout.CursorSizeCSVHeader(conf.outputCSVSeparator);
						Sys.Log2(200, $"WriteCursorSizeOutput [{name}] header [{header}]");
						sw.WriteLine(header);
						bHeaders = true;
					}
					row = tGoupOutout.CursorSizeCSVRow(conf.outputCSVSeparator);
					Sys.Log2(200, $"WriteCursorSizeOutput [{name}] row [{row}]");
					sw.WriteLine(row);
				}
			}

			swWriteFile.Stop();
			Sys.Log($"Create Cursor Size output file [{outputCursorSizeFilePath}] [{Sys.TimerGetText(swWriteFile.ElapsedMilliseconds)}]");
			swWriteFile.Reset();
		}

		public List<ThreadGroupOutput> GetOutputByStatus(bool success)
		{
			return _dOutput.Where(x => x.Value.querySuccess == success).Select(x => x.Value).ToList();
		}

		public List<ThreadGroupOutput> GetOuputByParsingError(bool error)
        {
			return _dOutput.Where(x => x.Value.parsingError == error).Select(x => x.Value).ToList();
		}

		public List<ThreadGroupOutput> GetOuputByDumpError(bool error)
		{
			return _dOutput.Where(x => x.Value.dumpError == error).Select(x => x.Value).ToList();
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

			if (p == SimpleStatProperty.RFMBoostExact && t == StatType.min) return l.Min(x => x.IQLRFMBoostExact);
			if (p == SimpleStatProperty.RFMBoostExact && t == StatType.max) return l.Max(x => x.IQLRFMBoostExact);
			if (p == SimpleStatProperty.RFMBoostExact && t == StatType.avg) return l.Average(x => x.IQLRFMBoostExact);
			if (p == SimpleStatProperty.RFMBoostExact && t == StatType.stddev) return StdDev(l.Select(x => x.IQLRFMBoostExact));

			if (p == SimpleStatProperty.RFMBoostSimilar && t == StatType.min) return l.Min(x => x.IQLRFMBoostSimilar);
			if (p == SimpleStatProperty.RFMBoostSimilar && t == StatType.max) return l.Max(x => x.IQLRFMBoostSimilar);
			if (p == SimpleStatProperty.RFMBoostSimilar && t == StatType.avg) return l.Average(x => x.IQLRFMBoostSimilar);
			if (p == SimpleStatProperty.RFMBoostSimilar && t == StatType.stddev) return StdDev(l.Select(x => x.IQLRFMBoostSimilar));

			return -1;
		}

		private List<(string engine, string elem, double duration)>  GetOutputMultiStatUsingCache(MultiStatProperty p, string engineName, string elem)
        {
			List<(string engine, string elem, double duration)> lFlatten = new List<(string engine, string elem, double duration)>();
			if (_multiStatPropertyCache.ContainsKey(p))
			{
				lFlatten = _multiStatPropertyCache[p];
			}
			else
			{
				List<ThreadGroupOutput> l = GetOutputByStatus(true);
				if (l.Count == 0) return null;
				switch (p)
				{
					case MultiStatProperty.IQLSearchRWA: lFlatten = l.SelectMany(x => x.IQLSearchRWA).AsParallel().ToList(); break;
					case MultiStatProperty.IQLFullTextSearchRWA: lFlatten = l.SelectMany(x => x.IQLFullTextSearchRWA).AsParallel().ToList(); break;
					case MultiStatProperty.IQLExecuteDBQuery: lFlatten = l.SelectMany(x => x.IQLExecuteDBQuery).AsParallel().ToList(); break;
					case MultiStatProperty.IQLFetchingDBQuery: lFlatten = l.SelectMany(x => x.IQLFetchingDBQuery).AsParallel().ToList(); break;
					case MultiStatProperty.IQLAcqRLk: lFlatten = l.SelectMany(x => x.IQLAcqRLk).AsParallel().ToList(); break;
					case MultiStatProperty.IQLDistribution: lFlatten = l.SelectMany(x => x.IQLDistribution).AsParallel().ToList(); break;
					case MultiStatProperty.IQLCorrelation: lFlatten = l.SelectMany(x => x.IQLCorrelation).AsParallel().ToList(); break;
				}
				_multiStatPropertyCache.Add(p, lFlatten);
			}

			if (lFlatten.Count == 0) return null;
			List<(string engine, string elem, double duration)> lFiltered = lFlatten.Where(x => Str.EQNC(x.engine, engineName) && Str.EQNC(x.elem, elem)).AsParallel().ToList();
			if (lFiltered.Count == 0) return null;
			return lFiltered;
		}

		//elem can be: index, distribution or correlation
		public double GetOutputMultiStat(MultiStatProperty p, StatType t, string engineName, string elem)
		{
			List<(string engine, string elem, double duration)> l = GetOutputMultiStatUsingCache(p, engineName, elem);
			if (l == null) return -1;

			if (t == StatType.min) return l.Min(x => x.duration);
			if (t == StatType.max) return l.Max(x => x.duration);
			if (t == StatType.avg) return l.Average(x => x.duration);
			if (t == StatType.stddev) return StdDev(l.Select(x => x.duration));

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
			switch (p)
			{
				case SimpleStatProperty.processingTime: lValues = l.Select(x => x.processingTime).ToList(); break;
				case SimpleStatProperty.cursorSize: lValues = l.Select(x => x.cursorSize).ToList(); break;
				case SimpleStatProperty.cursorSizeMB: lValues = l.Select(x => x.cursorSizeMB).ToList(); break;
				case SimpleStatProperty.clientFromPool: lValues = l.Select(x => x.clientFromPool).ToList(); break;
				case SimpleStatProperty.clientToPool: lValues = l.Select(x => x.clientToPool).ToList(); break;
				case SimpleStatProperty.rowFetchTime: lValues = l.Select(x => x.rowFetchTime).ToList(); break;
				case SimpleStatProperty.matchingRowCount: lValues = l.Select(x => x.matchingRowCount).ToList(); break;
				case SimpleStatProperty.postGroupByMatchingRowCount: lValues = l.Select(x => x.postGroupByMatchingRowCount).ToList(); break;
				case SimpleStatProperty.totalQueryTime: lValues = l.Select(x => x.totalQueryTime).ToList(); break;
				case SimpleStatProperty.readCursor: lValues = l.Select(x => x.readCursor).ToList(); break;
				case SimpleStatProperty.curosrNetworkAndDeserialization: lValues = l.Select(x => x.curosrNetworkAndDeserialization).ToList(); break;
				case SimpleStatProperty.RFMBoostExact: lValues = l.Select(x => x.IQLRFMBoostExact).ToList(); break;
				case SimpleStatProperty.RFMBoostSimilar: lValues = l.Select(x => x.IQLRFMBoostSimilar).ToList(); break;
				default: return -1;
			}

			List<double> lSortedValues = lValues.OrderBy(d => d).ToList();
			double minValue = lSortedValues.First(); 
			double maxValue = lSortedValues.Last();
			int lSortedValuesCount = lSortedValues.Count;

			int index = (int)Math.Ceiling(percentil * lSortedValues.Count);
			if (index <= 0) return minValue;
			if (index >= lSortedValues.Count) return maxValue;
			return lSortedValues.ElementAt(index);
		}

		public double GetOutputMultiStatPercentile(MultiStatProperty p, double percentil, string engineName, string elem)
		{
			List<(string engine, string elem, double duration)> l = GetOutputMultiStatUsingCache(p, engineName, elem);
			if (l == null) return -1;

			List<double> lSortedValues = l.Select(x => x.duration).OrderBy(d => d).ToList();
			double maxValue = lSortedValues.Last();
			int lSortedValuesCount = lSortedValues.Count;

			int index = (int)Math.Ceiling(percentil * lSortedValuesCount);
			if (index >= lSortedValuesCount) return maxValue;
			return lSortedValues.ElementAt(index);
		}

		public double GetQPS()
		{
			List<ThreadGroupOutput> l = GetOutputByStatus(true);
			return ((double)(GetOutputByStatus(true).Count) / ((double)(_stopWatch.ElapsedMilliseconds) / 1000));
		}

		public double GetPercentageSuccessQueries()
		{
			return ((GetOutputByStatus(true).Count() * 100) / outputCount);
		}

		public double GetPercentageFailedQueries()
		{
			return ((GetOutputByStatus(false).Count() * 100) / outputCount);
		}

		public double GetPercentageParsingErrorQueries()
		{
			return ((GetOuputByParsingError(true).Count() * 100) / outputCount);
		}

		public double GetPercentageDumpErrorQueries()
		{
			return ((GetOuputByDumpError(true).Count() * 100) / outputCount);
		}

		public List<(string row, string column, string value)> GetTableRowMultiStat(MultiStatProperty property, string engine, string elem, string format, bool min = true, bool avg = true, bool max = true, bool stdDev = true, bool percentil = true)
		{
			List<(string row, string column, string value)> tableRow = new List<(string row, string column, string value)>();
			string propertyName = $"[{elem}] [{engine}]";
			if (min)
			{
				double d = GetOutputMultiStat(property, StatType.min, engine, elem);
				tableRow.Add((propertyName, "MIN", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (avg)
			{
				double d = GetOutputMultiStat(property, StatType.avg, engine, elem);
				tableRow.Add((propertyName, "AVG", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (max)
			{
				double d = GetOutputMultiStat(property, StatType.max, engine, elem);
				tableRow.Add((propertyName, "MAX", d == -1 ? Str.Empty : d.ToString(format)));
			}
			if (stdDev)
			{
				double d = GetOutputMultiStat(property, StatType.stddev, engine, elem);
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
				foreach (double dPercentil in lPercentil)
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
			Sys.Log($"Thread Group [{name}] - Execution time [{Sys.TimerGetText(_stopWatch.ElapsedMilliseconds)}]");
			Sys.Log($"Thread Group [{name}] - Number of iterations [{outputCount}]");
			Sys.Log($"Thread Group [{name}] - Number of success queries [{GetOutputByStatus(true).Count()}/{outputCount}] [{GetPercentageSuccessQueries().ToString(_statsFormatPercent)}%]");
			Sys.Log($"Thread Group [{name}] - Number of failed queries [{GetOutputByStatus(false).Count()}/{outputCount}] [{GetPercentageFailedQueries().ToString(_statsFormatPercent)}%]");
			Sys.Log($"Thread Group [{name}] - Number of parsing error queries [{GetOuputByParsingError(true).Count()}/{outputCount}] [{GetPercentageParsingErrorQueries().ToString(_statsFormatPercent)}%]");
			Sys.Log($"Thread Group [{name}] - Number of dump error queries [{GetOuputByDumpError(true).Count()}/{outputCount}] [{GetPercentageDumpErrorQueries().ToString(_statsFormatPercent)}%]");
			Sys.Log($"Thread Group [{name}] - QPS (Query Per Second) [{GetQPS().ToString(_statsFormatQPS)}] ");

			if (conf.outputQueryTimers)
			{
				LogTable logTableTimers = new LogTable($"[{name}]");
				logTableTimers.SetInnerColumnSpaces(1, 1);

				//totalQueryTime
				logTableTimers.AddUniqueItem("totalQueryTime", "unit", "ms");
				logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.totalQueryTime, _statsFormatMs));
				//processingTime
				logTableTimers.AddUniqueItem("processingTime", "unit", "ms");
				logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.processingTime, _statsFormatMs));
				//rowFetchTime
				logTableTimers.AddUniqueItem("rowFetchTime", "unit", "ms");
				logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.rowFetchTime, _statsFormatMs));
				//readCursor
				logTableTimers.AddUniqueItem("readCursor", "unit", "ms");
				logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.readCursor, _statsFormatMs));
				//cursorSizeMB
				logTableTimers.AddUniqueItem("cursorSizeMB", "unit", "MB");
				logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.cursorSizeMB, _statsFormatMB));

				if (conf.outputQueryInfo)
				{
					//matchingRowCount
					logTableTimers.AddUniqueItem("matchingRowCount", "unit", "rows");
					logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.matchingRowCount, _statsFormatCount));
					//postGroupByMatchingRowCount
					logTableTimers.AddUniqueItem("postGroupByMatchingRowCount", "unit", "rows");
					logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.postGroupByMatchingRowCount, _statsFormatCount));
				}
				if (conf.outputClientTimers)
				{
					//clientFromPool
					logTableTimers.AddUniqueItem("clientFromPool", "unit", "ms");
					logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.clientFromPool, _statsFormatMs));
					//clientToPool
					logTableTimers.AddUniqueItem("clientToPool", "unit", "ms");
					logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.clientToPool, _statsFormatMs));
				}
				if (conf.outputCurosrNetworkAndDeserializationTimer)
				{
					//curosrNetworkAndDeserialization
					logTableTimers.AddUniqueItem("curosrNetworkAndDeserialization", "unit", "ms");
					logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.curosrNetworkAndDeserialization, _statsFormatMs));
				}
                if (conf.outputRFMBoost)
                {
					logTableTimers.AddUniqueItem("RFMBoostExact", "unit", "ms");
					logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.RFMBoostExact, _statsFormatMs));
					logTableTimers.AddUniqueItem("RFMBoostSimilar", "unit", "ms");
					logTableTimers.AddItems(GetTableRowSimpleStat(SimpleStatProperty.RFMBoostSimilar, _statsFormatMs));
				}
				
				logTableTimers.SysLog();
			}

			if (conf.outputIQLSearchRWA)
			{
				LogTable logTableQueryIQLSearchRWATimers = new LogTable($"[{name}] SearchRWA");
				logTableQueryIQLSearchRWATimers.SetInnerColumnSpaces(1, 1);
				LogTable logTableQueryIQLFullTextSearchRWATimers = new LogTable($"[{name}] FullTextSearchRWA");
				logTableQueryIQLFullTextSearchRWATimers.SetInnerColumnSpaces(1, 1);

				foreach ((string index, string engine) x in lSortedEngineIndex)
				{
					//SearchRWA
					logTableQueryIQLSearchRWATimers.AddUniqueItem($"[{x.index}] [{x.engine}]", "unit", "ms");
					logTableQueryIQLSearchRWATimers.AddItems(GetTableRowMultiStat(MultiStatProperty.IQLSearchRWA, x.engine, x.index, _statsFormatMs));
					//FullTextSearchRWA
					logTableQueryIQLFullTextSearchRWATimers.AddUniqueItem($"[{x.index}] [{x.engine}]", "unit", "ms");
					logTableQueryIQLFullTextSearchRWATimers.AddItems(GetTableRowMultiStat(MultiStatProperty.IQLFullTextSearchRWA, x.engine, x.index, _statsFormatMs));
				}
				//SearchRWA
				logTableQueryIQLSearchRWATimers.SysLog();
				//FullTextSearchRWA
				logTableQueryIQLFullTextSearchRWATimers.SysLog();
			}

			if (conf.outputIQLDBQuery)
			{
				LogTable logTableQueryIQLExecuteDBQueryTimers = new LogTable($"[{name}] ExecuteDBQuery");
				logTableQueryIQLExecuteDBQueryTimers.SetInnerColumnSpaces(1, 1);
				LogTable logTableQueryIQLFetchingDBQueryTimers = new LogTable($"[{name}] FetchingDBQuery");
				logTableQueryIQLFetchingDBQueryTimers.SetInnerColumnSpaces(1, 1);

				foreach ((string index, string engine) x in lSortedEngineIndex)
				{
					//ExecuteDBQuery
					//add count column
					List<(string engine, string elem, double duration)> lElems = GetOutputMultiStatUsingCache(MultiStatProperty.IQLExecuteDBQuery, x.engine, x.index);
					int elemCount = lElems == null ? 0 : lElems.Count();
					logTableQueryIQLExecuteDBQueryTimers.AddUniqueItem($"[{x.index}] [{x.engine}]", "count", elemCount.ToString(_statsFormatCount));
					//usual stats
					logTableQueryIQLExecuteDBQueryTimers.AddUniqueItem($"[{x.index}] [{x.engine}]", "unit", "ms");
					logTableQueryIQLExecuteDBQueryTimers.AddItems(GetTableRowMultiStat(MultiStatProperty.IQLExecuteDBQuery, x.engine, x.index, _statsFormatMs));
					//FetchingDBQuery
					logTableQueryIQLFetchingDBQueryTimers.AddUniqueItem($"[{x.index}] [{x.engine}]", "unit", "ms");
					logTableQueryIQLFetchingDBQueryTimers.AddItems(GetTableRowMultiStat(MultiStatProperty.IQLFetchingDBQuery, x.engine, x.index, _statsFormatMs));
				}
				//ExecuteDBQuery
				logTableQueryIQLExecuteDBQueryTimers.SysLog();
				//FetchingDBQuery
				logTableQueryIQLFetchingDBQueryTimers.SysLog();
			}

            if (conf.outputIQLAcqRLk)
            {
				//AcqRLk
				LogTable logTableQueryIQLAcqRLkTimers = new LogTable($"[{name}] AcqRLk");
				logTableQueryIQLAcqRLkTimers.SetInnerColumnSpaces(1, 1);

				foreach ((string index, string engine) x in lSortedEngineIndex)
				{
					logTableQueryIQLAcqRLkTimers.AddItems(GetTableRowMultiStat(MultiStatProperty.IQLAcqRLk, x.engine, x.index, _statsFormatMs));
				}

				logTableQueryIQLAcqRLkTimers.SysLog();
			}

			if (conf.outputIQLDistributionsCorrelations)
			{
				//Distribution
				LogTable logTableQueryIQLDistributionTimers = new LogTable($"[{name}] Distribution");
				logTableQueryIQLDistributionTimers.SetInnerColumnSpaces(1, 1);

				foreach ((string dist, string engine) x in lSortedEngineDistribution)
				{
					logTableQueryIQLDistributionTimers.AddUniqueItem($"[{x.dist}] [{x.engine}]", "unit", "ms");
					logTableQueryIQLDistributionTimers.AddItems(GetTableRowMultiStat(MultiStatProperty.IQLDistribution, x.engine, x.dist, _statsFormatMs));
				}
				logTableQueryIQLDistributionTimers.SysLog();


				//Correlation
				LogTable logTableQueryIQLCorrelationTimers = new LogTable($"[{name}] Correlation");
				logTableQueryIQLCorrelationTimers.SetInnerColumnSpaces(1, 1);

				List<(string row, string column, string value)> lQueryIQLCorrelationTimers = new List<(string row, string column, string value)>();
				foreach ((string correl, string engine) x in lSortedEngineCorrelation)
				{
					logTableQueryIQLCorrelationTimers.AddUniqueItem($"[{x.correl}] [{x.engine}]", "unit", "ms");
					logTableQueryIQLCorrelationTimers.AddItems(GetTableRowMultiStat(MultiStatProperty.IQLCorrelation, x.engine, x.correl, _statsFormatMs));
				}
				
				logTableQueryIQLCorrelationTimers.SysLog();
			}

				swStats.Stop();
			Sys.Log($"Thread Group [{name}] - Compute stats [{Sys.TimerGetText(swStats.ElapsedMilliseconds)}]");
		}

		#endregion
	}

	public class ParameterSet : IDoc
    {
		private Dictionary<string, string> _dParams = new Dictionary<string, string>();

		public string GetValue(string name)
        {
			if (String.IsNullOrEmpty(name))
			{
				throw new Exception("Key is null or empty");
			}
			if(_dParams.TryGetValue(name, out string value))
            {
				return value;
            }
			//variable not found, return {variable}
			//highlight(Text,'chunk=sentence/window,count=10,context.window=3,offsets=true,separator=;,startmarker="{b}",endmarker="{nb}",remap=true,dedup=1') as extracts
			else
			{
				return $"{{{name}}}";
            }
		}

		public bool SetValue(string name, string value)
		{
			if (String.IsNullOrEmpty(name))
            {
				throw new Exception("Key is null or empty");
			}
			if (_dParams.ContainsKey(name)) 
			{
				throw new Exception("Key already exist");
			}
			_dParams.Add(name, value);
			return true;
		}

		public object GetObject(string value)
		{
			throw new Exception("Not implemented");
		}

		public bool SetObject(string value, object obj)
        {
			throw new Exception("Not implemented");
		}
	}

}
