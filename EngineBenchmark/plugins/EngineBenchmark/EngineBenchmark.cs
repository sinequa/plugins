using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sinequa.Common;
using Sinequa.Configuration;
using Sinequa.Plugins;

namespace Sinequa.Plugin
{

	public class EngineBenchmark : CommandPlugin
	{
		private EngineActivityManager _engineActivityManager;
		private CmdConfigEngineBenchmark _conf;
		public CmdConfigEngineBenchmark conf
        {
			get { return _conf; }
        }

		public override Return OnPreExecute()
		{
			_conf = new CmdConfigEngineBenchmark(this);
			if (!_conf.LoadConfig()) return Return.Error;

			//load config (principals) if a thread group recquire user ACLs
			if (_conf.threadGroups.Any(x => x.Value.addUserACLs))
			{
				Sys.Log($"Start loading configuration and domains");
				if (!Application.InitWith(Application.Feature.Default | Application.Feature.Domains)) return Return.Error;
			}

			//command output log folder
			if (!Toolbox.CreateDir(_conf.outputFolderPath)) return Return.Error;

			//start PeriodicEngineActivity task
			if (_conf.activityMonitoring)
			{
				_engineActivityManager = new EngineActivityManager(_conf.activitylEngines, _conf.activityFrequency, _conf.activityDump, this);
				_engineActivityManager.StartPeriodicEngineActivity();
			}

			return base.OnPreExecute();
		}

		public override Return OnExecute()
		{
			ConcurrentDictionary<int, ThreadGroupOutput> dOutput = new ConcurrentDictionary<int, ThreadGroupOutput>();

			int parallelThreadGroup = 1;
			Sys.Log($"----------------------------------------------------");
			if (_conf.threadGroupsInParallel)
			{
				parallelThreadGroup = _conf.threadGroups.Count;
				Sys.Log($"Execute thread groups in parallel. Number of threads groups in parallel [{_conf.threadGroups.Count}]");
			}
			else
			{
				Sys.Log($"Execute thread groups sequentially. Number of threads groups to execute [{_conf.threadGroups.Count}]");
			}
			Sys.Log($"----------------------------------------------------");

			Parallel.ForEach(_conf.threadGroups.Values, new ParallelOptions { MaxDegreeOfParallelism = parallelThreadGroup }, (tGroup, threadGroupsLoopState) =>
			{
				Thread threadGroupsThread = Thread.CurrentThread;
				int threadGroupsThreadId = threadGroupsThread.ManagedThreadId;

				if (!tGroup.Init(this, _conf))
				{
					Sys.LogError($"{{{threadGroupsThreadId}}} Cannot init Thread Group [{tGroup.name}]");
					threadGroupsLoopState.Stop();
					return;
				}

				tGroup.stopWatch.Start();
				Sys.Log($"----------------------------------------------------");
				Sys.Log($"{{{threadGroupsThreadId}}} Thread Group [{tGroup.name}] start");
				Sys.Log($"----------------------------------------------------");

				Parallel.ForEach(Infinite(tGroup), new ParallelOptions { MaxDegreeOfParallelism = tGroup.threadNumber }, (ignore, threadGroupLoopState) =>
				{
					Thread threadGroupThread = Thread.CurrentThread;
					int threadGroupThreadId = threadGroupThread.ManagedThreadId;

					//increment number iterations
					int i = Interlocked.Increment(ref tGroup.nbIterration);

					//reached max iteration or max execution time - stop Parallel.ForEach
					if (i == tGroup.maxIteration || tGroup.stopWatch.ElapsedMilliseconds >= tGroup.maxExecutionTime)
					{
						threadGroupLoopState.Stop();
						if (i == tGroup.maxIteration)
						{
							Sys.Log($"{{{threadGroupThreadId}}} Thread group [{tGroup.name}] max iteration reached [{tGroup.maxIteration}], stop threads execution");
						}
						if (tGroup.stopWatch.ElapsedMilliseconds >= tGroup.maxExecutionTime)
						{
							Sys.Log($"{{{threadGroupThreadId}}} Thread group [{tGroup.name}] max execution time reached [" + tGroup.maxExecutionTime + " ms], stop threads execution");
						}
					}

					//Pause current thread based on random time
					int sleepTime = tGroup.GetSleep();
					Sys.Log2(20, $"{{{threadGroupThreadId}}} Thread group [{tGroup.name}][{i}] sleep [{sleepTime}]");
					Thread.Sleep(sleepTime);

					//get SQL query - SQL query variables have been replaced by parameters based on parameter strategy
					string sql = tGroup.GetSQLQuery(out Dictionary<string, string> dParams);
					Sys.Log2(20, $"{{{threadGroupThreadId}}} Thread group [{tGroup.name}][{i}] sql [{sql}]");
					//get Engine for EngineClient based on Engine Strategy
					string engineName = tGroup.GetEngine();
					Sys.Log2(20, $"{{{threadGroupThreadId}}} Thread group [{tGroup.name}][{i}] engine [{engineName}]");
					//Executy query
					Sys.Log2(10, $"{{{threadGroupThreadId}}} Thread group [{tGroup.name}][{i}] prepare execute SQL on engine [{engineName}] with parameters " + String.Join(";", dParams.Select(x => $"[${ x.Key }$]={x.Value}").ToArray()));
					DateTime dStart = DateTime.Now;

					//execute SQL query
					BenchmarkQuery query = new BenchmarkQuery(threadGroupThreadId, tGroup.name, i, engineName, sql, tGroup);
					query.Execute();

					DateTime dEnd = DateTime.Now;
					Sys.Log($"{{{threadGroupThreadId}}} Thread group [{tGroup.name}][{i}] execute SQL on engine [{engineName}], success [{query.success}] total query time [{Sys.TimerGetText(query.totalQueryTimer)}] processing time [{Sys.TimerGetText(query.processingTime)}]");

					//Store execution result in output
					if (tGroup.AddOutput(i, threadGroupThreadId, dStart, dEnd, out ThreadGroupOutput tGroupOutput))
					{
						//optimize memory usage, store only values needed for output
						tGroupOutput.SetSuccess(query.success);
						tGroupOutput.SetInfo(engineName, dParams, query.cursorSize);
						if (_conf.outputSQLQuery) tGroupOutput.SetSQL(sql);
						if (_conf.outputClientTimers) tGroupOutput.SetClientTimers(query.clientFromPoolTimer, query.clientToPoolTimer);
						if (_conf.outputQueryTimers) tGroupOutput.SetQueryTimers(query.processingTime, query.cacheHit, query.rowFetchTime, query.matchingRowCount, query.postGroupByMatchingRowCount, query.totalQueryTimer, query.readCursorTimer);
						if (_conf.outputCurosrNetworkAndDeserializationTimer) tGroupOutput.SetCursorNetworkAndDeserialization(query.cursorNetworkAndDeserializationTimer);
						if (_conf.outputIQL) tGroupOutput.SetInternalQueryLog(query.internalQueryLog);
						if (_conf.outputCursorSizeBreakdown) tGroupOutput.SetCursorSizeBreakdown(query.cursorSizeBreakdown);
						if (_conf.dumpIQL) tGroupOutput.DumpInternalQueryLog(tGroup.name, query.internalQueryLog, i, query.processingTime);
						if (_conf.dumpIQA) tGroupOutput.DumpInternalQueryAnalysis(tGroup.name, query.internalQueryAnalysis, i, query.processingTime);
					}

					//dispose BenchmarkQuery
					query.Dispose();
				});

				tGroup.stopWatch.Stop();
				Sys.Log($"----------------------------------------------------");
				Sys.Log($"{{{threadGroupsThreadId}}} Thread Group [{tGroup.name}] stop");
				Sys.Log($"----------------------------------------------------");
			});

			return base.OnExecute();
		}

		public override Return OnPostExecute(bool execute_ok)
		{
			bool bHeaders = false;
			Stopwatch swWriteFile = new Stopwatch();

			//stop PeriodicEngineActivity task
			if (_conf.activityMonitoring) _engineActivityManager.StopPeriodicEngineActivity();

			//log Thread Groups statistics
			foreach (ThreadGroup tGroup in _conf.threadGroups.Values) tGroup.LogQueriesStats();

			//log Engine Activity statistics
			_engineActivityManager.LogStats();

			//ouput queries csv
			if (_conf.outputQueries)
			{
				Sys.Log($"----------------------------------------------------");
				foreach (ThreadGroup tGroup in _conf.threadGroups.Values)
				{
					tGroup.WriteQueryOutpout();
				}
				Sys.Log($"----------------------------------------------------");
			}

			//ouput cursor size breakdown
			if (_conf.outputCursorSizeBreakdown)
			{
				Sys.Log($"----------------------------------------------------");
				foreach (ThreadGroup tGroup in _conf.threadGroups.Values)
				{
					tGroup.WriteCursorSizeOutput();
				}
				Sys.Log($"----------------------------------------------------");
			}

			//output engine activity
			if (_conf.activityMonitoring)
			{
				string outputEngineActivityFilePath = Toolbox.GetOutputFilePath(_conf.outputFolderPath, "EngineActivity", "csv");
				swWriteFile.Start();
				bHeaders = false;

				using (StreamWriter sw = new StreamWriter(outputEngineActivityFilePath, false, Encoding.UTF8))
				{
					foreach (EngineActivityItem item in _engineActivityManager.lEngineActivityItem)
					{
						if (!bHeaders)
						{
							sw.WriteLine(item.ToCSVHeaders(_conf.outputCSVSeparator));
							bHeaders = true;
						}
						sw.WriteLine(item.ToCSV(_conf.outputCSVSeparator));
					}
				}

				swWriteFile.Stop();
				Sys.Log($"----------------------------------------------------");
				Sys.Log($"Create Engine Activity output file [{outputEngineActivityFilePath}] [{Sys.TimerGetText(swWriteFile.ElapsedMilliseconds)}]");
				Sys.Log($"----------------------------------------------------");
				swWriteFile.Reset();
			}

			return base.OnPostExecute(execute_ok);
		}

		private static IEnumerable<bool> Infinite(ThreadGroup tGroup)
		{
			while (true)
			{
				yield return true;
			}
		}

	}

	public class CmdConfigEngineBenchmark
	{
		private XDoc _XMLConf = null;
		private EngineBenchmark _engineBenchmark = null;
		private IDocContext _ctxt = new IDocContext();

		public DateTime startTime { get; private set; }

		//engine config
		public ListOf<CCEngine> lEngines { get; private set; }
		public EngineStategy engineStategy { get; private set; }

		//thread groups
		public bool threadGroupsInParallel { get; private set; }
		public Dictionary<string, ThreadGroup> threadGroups { get; private set; } = new Dictionary<string, ThreadGroup>();

		//users ACls
		public SecuritySyntax securitySyntax { get; private set; }
		public CCDomain domain { get; private set; }
		public ListStr lUsers { get; private set; } = new ListStr();

		//output
		private string _outputFolderPath;
		public string outputFolderPath
		{
			get
			{
				return Str.PathAdd(_outputFolderPath, _engineBenchmark.Command.Name + "_" + startTime.ToString("yyyy-MM-dd HH-mm-ss"));
			}
		}
		public char outputCSVSeparator { get; private set; }
		public bool outputQueries { get; private set; }
		public bool outputSQLQuery { get; private set; }
		public bool outputQueryTimers { get; private set; }
		public bool outputQueryInfo { get; private set; }
		public bool outputClientTimers { get; private set; }
		public bool outputCurosrNetworkAndDeserializationTimer { get; private set; }
		public bool outputParameters { get; private set; }
		//output internal query 
		public bool outputIQL { get; private set; }
		public bool outputIQLThreadCount { get; private set; }
		public bool outputIQLSearchRWA { get; private set; }
		public bool outputIQLFullTextSearchRWA { get; private set; }
		public bool outputIQLDBQuery { get; private set; }
		public bool outputIQLHeader { get; private set; }
		public bool outputIQLBrokering { get; private set; }
		public bool outputIQLDistributionsCorrelations { get; private set; }

		//cursor size breakdown
		public bool outputCursorSizeBreakdown { get; private set; }
		public readonly string outputCursorRowCount = "Cursor Rows Count";
		public bool outputCursorSizeEmptyColumns { get; private set; }

		//dump
		public bool dumpIQL { get; private set; }
		public int dumpIQLMinProcessingTime { get; private set; }
		public bool dumpIQA { get; private set; }
		public int dumpIQAMinProcessingTime { get; private set; }
		public bool dumpCursor { get; private set; }
		public int dumpCursorMinSize { get; private set; }
		public int dumpCursorMinProcessingTime { get; private set; }

		//activity
		public bool activityMonitoring { get; private set; }
		public ListOf<CCEngine> activitylEngines { get; private set; }
		public int activityFrequency { get; private set; }
		public bool activityDump { get; private set; }

		//engine status
		public List<EngineCustomStatus> enginesStatus { get; private set; } = new List<EngineCustomStatus>();

		public CmdConfigEngineBenchmark(EngineBenchmark engineBenchmark)
		{
			this._engineBenchmark = engineBenchmark;
			this._XMLConf = engineBenchmark.Command.GetXDoc();
			this.startTime = DateTime.Now;
			_ctxt.Doc = new IDocImpl();
		}

		public bool LoadConfig()
		{
			string dataTag = Str.Empty;

			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Load configuration");
			Sys.Log($"----------------------------------------------------");

			if (_XMLConf == null)
			{
				Sys.LogError($"Cannot read configuration");
				return false;
			}

			#region engines
			//engines
			dataTag = "CMD_ENGINE_LIST";
			if (!DatatagExist(dataTag)) return false;
			string engines = _XMLConf.Value(dataTag, null);
			if (String.IsNullOrEmpty(engines) || String.IsNullOrWhiteSpace(engines))
			{
				Sys.LogError($"Invalid configuration property: Engines configuration - Engines is empty");
				return false;
			}
			lEngines = CC.Current.Engines.CleanAliasList3(engines);
			if (lEngines == null || lEngines.Count == 0)
			{
				Sys.LogError($"Invalid configuration property: Engines configuration - Resolve Engine aliases error");
				return false;
			}

			//egine stratategy
			dataTag = "CMD_ENGINE_STRATEGY";
			if (!DatatagExist(dataTag)) return false;
			string esType = _XMLConf.Value(dataTag, Str.Empty);
			if (String.IsNullOrEmpty(esType))
			{
				Sys.LogError($"Invalid configuration property: Engines configuration - Engine stategy is empty");
				return false;
			}
			if (Enum.TryParse(esType, out EngineStategy est))
			{
				this.engineStategy = est;
			}
			else
			{
				Sys.LogError($"Invalid configuration property: Engines configuration - Engine stategy type [{esType}]");
				return false;
			}
			#endregion

			#region thread groups
			//thread group in parallel
			dataTag = "CMD_THREAD_GROUP_PARALLEL";
			if (!DatatagExist(dataTag)) return false;
			threadGroupsInParallel = _XMLConf.ValueBoo(dataTag, false);

			//thread groups grid
			dataTag = "CMD_THREAD_GROUP_GRID";
			if (!DatatagExist(dataTag, false))
			{
				Sys.LogError($"Invalid configuration property: You need to create a thread group");
				return false;
			}
			ListOf<XDoc> lItemsGridThreadGroup = _XMLConf.EltList("CMD_THREAD_GROUP_GRID");
			for (int i = 0; i < lItemsGridThreadGroup.Count; i++)
			{
				XDoc itemGridThreadGroup = lItemsGridThreadGroup.Get(i);

				//Name
				string name = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_NAME");
				if (String.IsNullOrEmpty(name) || String.IsNullOrWhiteSpace(name))
				{
					Sys.LogError($"Invalid configuration property: Thread group - name is empty");
					return false;
				}

				//SQL
				string SQL = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_SQL");
				if (String.IsNullOrEmpty(SQL) || String.IsNullOrWhiteSpace(SQL))
				{
					Sys.LogError($"Invalid configuration property: Thread group - SQL is empty");
					return false;
				}

				//parameter files
				string paramCustomFileName = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_PARAM_CUSTON_FILE");
				if (String.IsNullOrEmpty(paramCustomFileName) || String.IsNullOrWhiteSpace(paramCustomFileName))
				{
					Sys.LogError($"Invalid configuration property: Thread group - Parameter file is empty");
					return false;
				}
				if (!CC.Current.FileExist("customfile", paramCustomFileName))
				{
					Sys.LogError($"Invalid configuration property: Thread group - Parameter file, custom file [{paramCustomFileName}] not found");
					return false;
				}
				CCFile paramCustomFile = CC.Current.FileGet("customfile", paramCustomFileName);

				//file sep
				char fileSep = itemGridThreadGroup.ValueChar("CMD_THREAD_GROUP_GRID_PARAM_CUSTON_FILE_SEP", ';');
				if (Char.IsWhiteSpace(fileSep))
				{
					Sys.LogError($"Invalid configuration property: Thread group - File separator cannot be a white space");
					return false;
				}

				//param stratategy
				string paramStrategy = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_PARAM_STRATEGY", Str.Empty);
				if (String.IsNullOrEmpty(paramStrategy))
				{
					Sys.LogError($"Invalid configuration property: Thread group - Parameter stategy is empty");
					return false;
				}
				ParameterStrategy parameterStrategy;
				if (!Enum.TryParse(paramStrategy, out parameterStrategy))
				{
					Sys.LogError($"Invalid configuration property: Thread group - Parameter stategy type [{paramStrategy}]");
					return false;
				}

				//user ACL
				bool usersACL = itemGridThreadGroup.ValueBoo("CMD_THREAD_GROUP_GRID_USERS_ACL", false);

				//Threads number
				int threadNumber = itemGridThreadGroup.ValueInt("CMD_THREAD_GROUP_GRID_THREADS_NUMBER", 5);
				if (threadNumber <= 0)
				{
					Sys.LogError($"Invalid configuration property: Thread group - Thread number must be > 0");
					return false;
				}

				//Threads sleep
				int threadSleepMin;
				int threadSleepMax;
				string threadSleep = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_THREADS_SLEEP", "3;10");
				if (String.IsNullOrEmpty(threadSleep) || String.IsNullOrWhiteSpace(threadSleep))
				{
					Sys.LogError($"Invalid configuration property: Thread group - Thread sleep is empty");
					return false;
				}
				string sleepMin = Str.ParseToSep(threadSleep, ';');
				string sleepMax = Str.ParseFromSep(threadSleep, ';');
				if (!int.TryParse(sleepMin, out threadSleepMin) || !int.TryParse(sleepMax, out threadSleepMax))
				{
					Sys.LogError($"Invalid configuration property: Thread group - Thread sleep format must be <min;max> where min and max integers");
					return false;
				}
				if (threadSleepMin > threadSleepMax)
				{
					Sys.LogError($"Invalid configuration property: Thread group - Thread sleep min > Thread sleep max");
					return false;
				}

				//Max execution time
				int maxExecutionTime = itemGridThreadGroup.ValueInt("CMD_THREAD_GROUP_GRID_MAX_TIME", 60);
				if (threadNumber <= 0)
				{
					Sys.LogError($"Invalid configuration property: Thread group -  Execution time must be > 0");
					return false;
				}

				//Max iteration
				int maxIterations = itemGridThreadGroup.ValueInt("CMD_THREAD_GROUP_GRID_MAX_ITERATION", 100);
				if (threadNumber <= 0)
				{
					Sys.LogError($"Invalid configuration property: Thread group - Max iteration must be > 0");
					return false;
				}

				ThreadGroup tg = new ThreadGroup(name, SQL, paramCustomFile, fileSep, parameterStrategy, usersACL, threadNumber,
					threadSleepMin, threadSleepMax, maxExecutionTime, maxIterations);
				threadGroups.Add(name, tg);
			}
			#endregion

			#region user ACL
			//load users only if a thread group use UserAcls
			if (threadGroups.Where(x => x.Value.addUserACLs == true).ToList().Count > 0)
			{
				//security syntax
				dataTag = "CMD_SECURITY_SYNTAX";
				if (!DatatagExist(dataTag)) return false;
				string ssType = _XMLConf.Value(dataTag, Str.Empty);
				if (String.IsNullOrEmpty(ssType))
				{
					Sys.LogError($"Invalid configuration property: User ACLs - Security syntax is empty");
					return false;
				}
				if (Enum.TryParse(ssType, out SecuritySyntax ss))
				{
					this.securitySyntax = ss;
				}
				else
				{
					Sys.LogError($"Invalid configuration property: User ACLs - Security syntax [{ssType}]");
					return false;
				}
				//Engine security only available >= 11.3.0
				if (this.securitySyntax == SecuritySyntax.Engine && (Sys.GetMajorVersion() < 11 || Sys.GetMinorVersion() < 3))
				{
					Sys.LogError($"Invalid configuration property: User ACLs - Security syntax, Engine security requires >= 11.3.0, current version [{Sys.GetVersion()}] ");
					return false;
				}

				//domain
				dataTag = "CMD_USERS_DOMAIN";
				if (!DatatagExist(dataTag)) return false;
				string securityDomain = _XMLConf.Value(dataTag, null);
				if (String.IsNullOrEmpty(securityDomain) || String.IsNullOrWhiteSpace(securityDomain))
				{
					Sys.LogError($"Invalid configuration property: User ACLs - Security domain is empty");
					return false;
				}
				if (!CC.Current.Domains.Exist(securityDomain))
				{
					Sys.LogError($"Invalid configuration property: User ACLs - Security domain not found [{securityDomain}]");
					return false;
				}
				domain = CC.Current.Domains.Get(securityDomain);

				//users grid
				dataTag = "CMD_USERS_GRID";
				if (!DatatagExist(dataTag, false))
				{
					Sys.LogError($"Invalid configuration property: User ACLs - user list is empty");
					return false;
				}
				ListOf<XDoc> lItemsGridUsers = _XMLConf.EltList("CMD_USERS_GRID");
				for (int i = 0; i < lItemsGridUsers.Count; i++)
				{
					XDoc itemGridUsers = lItemsGridUsers.Get(i);

					//user list
					string userId = itemGridUsers.Value("CMD_USERS_GRID_USER_ID", null);
					if (String.IsNullOrEmpty(userId) || String.IsNullOrWhiteSpace(userId))
					{
						Sys.LogError($"Invalid configuration property: User ACLs - user is empty");
						return false;
					}
					lUsers.AddUnique(userId);
				}
			}
			else   //default values
			{
				domain = null;
				lUsers = null;
			}
			#endregion

			#region output
			//output folder path
			dataTag = "CMD_OUTPUT_FOLDER_PATH";
			if (!DatatagExist(dataTag)) return false;
			_outputFolderPath = _XMLConf.Value(dataTag, null);
			if (String.IsNullOrEmpty(_outputFolderPath) || String.IsNullOrWhiteSpace(_outputFolderPath))
			{
				Sys.LogError($"Invalid configuration property: Output - Folder path is empty");
				return false;
            }
			//value pattern
			_outputFolderPath = IDocHelper.GetValuePattern(_ctxt, _outputFolderPath);

			//CSV separator
			dataTag = "CMD_OUTPUT_CSV_SEPARATOR";
			if (!DatatagExist(dataTag)) return false;
			outputCSVSeparator = _XMLConf.ValueChar(dataTag, '\t');
			#endregion

			#region output queries
			//output queries
			dataTag = "CMD_OUTPUT_QUERIES";
			if (!DatatagExist(dataTag)) return false;
			outputQueries = _XMLConf.ValueBoo(dataTag, true);

			//SQL Query
			dataTag = "CMD_OUTPUT_SQL_QUERY";
			if (!DatatagExist(dataTag)) return false;
			outputSQLQuery = _XMLConf.ValueBoo(dataTag, false);

			//query timers
			dataTag = "CMD_OUTPUT_QUERY_TIMERS";
			if (!DatatagExist(dataTag)) return false;
			outputQueryTimers = _XMLConf.ValueBoo(dataTag, true);

			//query info
			dataTag = "CMD_OUTPUT_QUERY_INFO";
			if (!DatatagExist(dataTag)) return false;
			outputQueryInfo = _XMLConf.ValueBoo(dataTag, true);

			//client timers
			dataTag = "CMD_OUTPUT_CLIENT_TIMERS";
			if (!DatatagExist(dataTag)) return false;
			outputClientTimers = _XMLConf.ValueBoo(dataTag, true);

			//network time
			dataTag = "CMD_OUTPUT_CURSOR_NETWORK_DESERIALIZATION_TIMER";
			if (!DatatagExist(dataTag)) return false;
			outputCurosrNetworkAndDeserializationTimer = _XMLConf.ValueBoo(dataTag, true);

			//parameters
			dataTag = "CMD_OUTPUT_PARAMETERS";
			if (!DatatagExist(dataTag)) return false;
			outputParameters = _XMLConf.ValueBoo(dataTag, true);

			//internal query - search RWA
			dataTag = "CMD_OUTPUT_INTERNALQUERYLOG_SEARCH_RWA";
			if (!DatatagExist(dataTag)) return false;
			outputIQLSearchRWA = _XMLConf.ValueBoo(dataTag, false);

			//internal query - Execute DBQuery & Fetching DBQuery
			dataTag = "CMD_OUTPUT_INTERNALQUERYLOG_DB_QUERY";
			if (!DatatagExist(dataTag)) return false;
			outputIQLDBQuery = _XMLConf.ValueBoo(dataTag, false);

			//internal query - query processor parse
			dataTag = "CMD_OUTPUT_INTERNALQUERYLOG_QUERY_HEADER";
			if (!DatatagExist(dataTag)) return false;
			outputIQLHeader = _XMLConf.ValueBoo(dataTag, false);

			//internal query - brokering - engines + MergeAttributes
			dataTag = "CMD_OUTPUT_INTERNALQUERYLOG_BROKERING";
			if (!DatatagExist(dataTag)) return false;
			outputIQLBrokering = _XMLConf.ValueBoo(dataTag, false);

			//internal query - Distributions and Correlations
			dataTag = "CMD_OUTPUT_INTERNALQUERYLOG_DISTRIBUTIONS_CORRELATIONS";
			if (!DatatagExist(dataTag)) return false;
			outputIQLDistributionsCorrelations = _XMLConf.ValueBoo(dataTag, false);

			//internal query - Threads count
			dataTag = "CMD_OUTPUT_INTERNALQUERYLOG_THREADS";
			if (!DatatagExist(dataTag)) return false;
			outputIQLThreadCount = _XMLConf.ValueBoo(dataTag, false);


			//if any outputIQL*, outputIQL = true
			if (outputIQLSearchRWA || outputIQLDBQuery || outputIQLHeader || outputIQLBrokering || outputIQLDistributionsCorrelations || outputIQLThreadCount)
			{
				outputIQL = true;
			}

			//add output timers & info recquires output queries file
			if (!outputQueries && (outputSQLQuery || outputQueryTimers || outputQueryInfo || outputClientTimers || outputCurosrNetworkAndDeserializationTimer || outputParameters || outputIQL))
			{
				Sys.LogError($"Cannot output queries timers and informations, you must enable 'queries output' first");
				return false;
			}

			#endregion

			#region output cursor size breakdown

			//cursor size breakdown
			dataTag = "CMD_OUTPUT_CURSOR_SIZE_BREAKDOWN";
			if (!DatatagExist(dataTag)) return false;
			outputCursorSizeBreakdown = _XMLConf.ValueBoo(dataTag, false);

			//empty columns (size 0)
			dataTag = "CMD_OUTPUT_CURSOR_SIZE_EMPTY_COLUMNS";
			if (!DatatagExist(dataTag)) return false;
			outputCursorSizeEmptyColumns = _XMLConf.ValueBoo(dataTag, false);

			#endregion

			#region dump
			//internal query log
			dataTag = "CMD_DUMP_INTERNALQUERYLOG_XML";
			if (!DatatagExist(dataTag)) return false;
			dumpIQL = _XMLConf.ValueBoo(dataTag, false);

			//internal query log min processing time
			dataTag = "CMD_DUMP_INTERNALQUERYLOG_XML_MIN_PROCESSING_TIME";
			if (!DatatagExist(dataTag)) return false;
			dumpIQLMinProcessingTime = _XMLConf.ValueInt(dataTag, 1000);

			//internal query analysis
			dataTag = "CMD_DUMP_INTERNALQUERYANALYSIS_XML";
			if (!DatatagExist(dataTag)) return false;
			dumpIQA = _XMLConf.ValueBoo(dataTag, false);

			//internal query analysis min processing time
			dataTag = "CMD_DUMP_INTERNALQUERYANALYSIS_XML_MIN_PROCESSING_TIME";
			if (!DatatagExist(dataTag)) return false;
			dumpIQAMinProcessingTime = _XMLConf.ValueInt(dataTag, 1000);

			//cursor
			dataTag = "CMD_DUMP_CURSOR";
			if (!DatatagExist(dataTag)) return false;
			dumpCursor = _XMLConf.ValueBoo(dataTag, false);

			//cursor min size
			dataTag = "CMD_DUMP_CURSOR_MIN_SIZE";
			if (!DatatagExist(dataTag)) return false;
			dumpCursorMinSize = _XMLConf.ValueInt(dataTag, 1);

			//cursor min processing time
			dataTag = "CMD_DUMP_CURSOR_MIN_PROCESSING_TIME";
			if (!DatatagExist(dataTag)) return false;
			dumpCursorMinProcessingTime = _XMLConf.ValueInt(dataTag, 1000);

			#endregion

			#region output activity
			//Enable Engine activity
			dataTag = "CMD_ACTIVITY_MONITORING";
			if (!DatatagExist(dataTag)) return false;
			activityMonitoring = _XMLConf.ValueBoo(dataTag, false);

			//activity Engines
			dataTag = "CMD_ACTIVITY_ENGINE_LIST";
			if (!DatatagExist(dataTag)) return false;
			string activityEngines = _XMLConf.Value(dataTag, null);
			if (activityMonitoring && (String.IsNullOrEmpty(activityEngines) || String.IsNullOrWhiteSpace(activityEngines)))
			{
				Sys.LogError($"Invalid configuration property: Activity Engines configuration - Engines is empty");
				return false;
			}
			activitylEngines = CC.Current.Engines.CleanAliasList3(activityEngines);
			if (activitylEngines == null || activitylEngines.Count == 0)
			{
				Sys.LogError($"Invalid configuration property: Activity Engines configuration - Resolve Engine aliases error");
				return false;
			}

			//Activity monitor frequency
			dataTag = "CMD_ACTIVITY_FREQUENCY";
			if (!DatatagExist(dataTag)) return false;
			activityFrequency = _XMLConf.ValueInt(dataTag, 1000);

			dataTag = "CMD_ACTIVITY_DUMP";
			if (!DatatagExist(dataTag)) return false;
			activityDump = _XMLConf.ValueBoo(dataTag, false);
			#endregion

			#region engine status
			//load engines status for all Engines in the Sinequa Grid. In case of brokering, other Engines than the ones defined in the "engine" list can be used
			if (!LoadEngineStatus())
			{
				Sys.LogError($"All Engines must be up and running in order to execute benchmark");
				return false;
			}
			#endregion

			Sys.Log($"Load configuration OK");

			LogConfig();

			return true;
		}

		private bool LoadEngineStatus()
		{
			foreach (CCEngine engine in CC.Current.Engines)
			{
				EngineCustomStatus ECS = Toolbox.GetEngineStatus(engine.FullName);
				if (ECS == null || !ECS.IsAlive)
				{
					Sys.LogError($"Can't connect to Engine [{engine.FullName}]");
					return false;
				}
				enginesStatus.Add(ECS);
			}
			return true;
		}

		public void LogConfig()
		{
			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Engines : [{String.Join(",", this.lEngines.Select(x => x.DisplayName).ToArray())}]");
			Sys.Log($"Engine strategy : [{Enum.GetName(typeof(EngineStategy), this.engineStategy)}]");
			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Execute thread groups in parallel : [{this.threadGroupsInParallel}]");
			foreach (string tgName in threadGroups.Keys)
			{
				threadGroups.TryGetValue(tgName, out ThreadGroup tg);
				Sys.Log($"Thread group [{tgName}] configuration:");
				tg.LogConf();
				Sys.Log($"----------------------------------------------------");
			}
			if (domain != null && lUsers != null && lUsers.Count > 0)
			{
				Sys.Log($"Security syntax : [{Enum.GetName(typeof(SecuritySyntax), this.securitySyntax)}]");
				Sys.Log($"Security domain [{domain.Name}]");
				Sys.Log($"Users [{lUsers.ToStr(';')}]");
				Sys.Log($"----------------------------------------------------");
			}
			Sys.Log($"Output folder path : [{this._outputFolderPath}]");
			Sys.Log($"Output CSV separator : [{this.outputCSVSeparator}]");
			Sys.Log($"Output queries : [{this.outputQueries}]");
			Sys.Log($"Output SQL query : [{this.outputSQLQuery}]");
			Sys.Log($"Output query timers : [{this.outputQueryTimers}]");
			Sys.Log($"Output query info : [{this.outputQueryInfo}]");
			Sys.Log($"Output client timers : [{this.outputClientTimers}]");
			Sys.Log($"Output curosr network & deserialization timer : [{this.outputCurosrNetworkAndDeserializationTimer}]");
			Sys.Log($"Output parameters : [{this.outputParameters}]");
			Sys.Log($"Output internal query log - search RWA timers : [{this.outputIQLSearchRWA}]");
			Sys.Log($"Output internal query log - DB Query timers : [{this.outputIQLDBQuery}]");
			Sys.Log($"Output internal query log - header timers: [{this.outputIQLHeader}]");
			Sys.Log($"Output internal query log - brokering info & timer : [{this.outputIQLBrokering}]");
			Sys.Log($"Output internal query log - distributions & correlations timers : [{this.outputIQLDistributionsCorrelations}]");
			Sys.Log($"Output internal query log - threads count : [{this.outputIQLThreadCount}]");
			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Output Cursor Size Breakdown : [{this.outputCursorSizeBreakdown}]");
			Sys.Log($"Output Cursor Size empty columns : [{this.outputCursorSizeEmptyColumns}]");
			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Dump internal query log : [{this.dumpIQL}]");
			if (this.dumpIQL) Sys.Log($"Dump internal query log min processing time : [{this.dumpIQLMinProcessingTime}] ms");
			Sys.Log($"Dump internal query analysis : [{this.dumpIQA}]");
			if (this.dumpIQA) Sys.Log($"Dump internal query analysis min processing time : [{this.dumpIQAMinProcessingTime}] ms");
			Sys.Log($"Dump cursor : [{this.dumpCursor}]");
			if (this.dumpCursor) Sys.Log($"Dump cursor min size : [{this.dumpCursorMinSize}] MB");
			if (this.dumpCursor) Sys.Log($"Dump cursor min processing time : [{this.dumpIQAMinProcessingTime}] ms");
			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Engine activity monitoring: [{this.activityMonitoring}]");
			if (this.activityMonitoring)
			{
				Sys.Log($"Engines : [{String.Join(",", this.activitylEngines.Select(x => x.DisplayName).ToArray())}]");
				Sys.Log($"Frequency : [{this.activityFrequency}] ms");
				Sys.Log($"Dump : [{this.activityDump}]");
			}
			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Engine(s) Status");
			foreach (EngineCustomStatus ECS in enginesStatus) ECS.Log(true);
			Sys.Log($"----------------------------------------------------");
		}

		private bool DatatagExist(string dataTag, bool logError = true)
		{
			if (!_XMLConf.EltExist(dataTag))
			{
				if (logError) Sys.LogError($"Invalid configuration property, datatag {dataTag} is missing");
				return false;
			}
			return true;
		}
	}

}
