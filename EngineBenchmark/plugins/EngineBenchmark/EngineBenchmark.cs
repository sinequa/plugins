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
using System.Xml;
using Sinequa.Common;
using Sinequa.Configuration;
using Sinequa.Plugins;
using Sinequa.Search;


namespace Sinequa.Plugin
{

	public class EngineBenchmark : CommandPlugin
	{
		CmdConfigEngineBenchmark conf;

		public override Return OnPreExecute()
		{
			conf = new CmdConfigEngineBenchmark(this.Command.Doc);
			if (!conf.LoadConfig()) return Return.Error;

			//load config (principals) if a thread group recquire user ACLs
			if (conf.threadGroups.Any(x => x.Value.addUserACLs))
			{
				Sys.Log("Start loading configuration and domains");
				Application.SetNeeds(true, true, true, true, false);
				if(!CC.Init(true)) return Return.Error;
			}

			return base.OnPreExecute();
		}

		public override Return OnExecute()
		{

			ConcurrentDictionary<int, ThreadGroupOutput> dOutput = new ConcurrentDictionary<int, ThreadGroupOutput>();

			int parallelThreadGroup = 1;
			Sys.Log("----------------------------------------------------");
			if (conf.threadGroupsInParallel)
			{
				parallelThreadGroup = conf.threadGroups.Count;
				Sys.Log("Execute thread groups in parallel. Number of threads groups in parallel [" + conf.threadGroups.Count + "]");
			}
			else
			{
				Sys.Log("Execute thread groups sequentially. Number of threads groups to execute [" + conf.threadGroups.Count + "]");
			}
			Sys.Log("----------------------------------------------------");

			Parallel.ForEach(conf.threadGroups.Values, new ParallelOptions { MaxDegreeOfParallelism = parallelThreadGroup }, (tGroup, threadGroupsLoopState) =>
			{
				Thread threadGroupsThread = Thread.CurrentThread;
				int threadGroupsThreadId = threadGroupsThread.ManagedThreadId;

				if (!tGroup.Init(conf))
				{
					Sys.LogError("{" + threadGroupsThreadId + "} Cannot init Thread Group [" + tGroup.name + "]");
					threadGroupsLoopState.Stop();
					return;
				}

				tGroup.stopWatch.Start();
				Sys.Log("----------------------------------------------------");
				Sys.Log("{" + threadGroupsThreadId + "} Thread Group [" + tGroup.name + "] start");
				Sys.Log("----------------------------------------------------");

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
							Sys.Log("{" + threadGroupThreadId + "} Thread group [" + tGroup.name + "] max iteration reached [" + tGroup.maxIteration + "], stop threads execution");
						}
						if (tGroup.stopWatch.ElapsedMilliseconds >= tGroup.maxExecutionTime)
						{
							Sys.Log("{" + threadGroupThreadId + "} Thread group [" + tGroup.name + "] max execution time reached [" + tGroup.maxExecutionTime + " ms], stop threads execution");
						}
					}

					//Pause current thread based on random time
					int sleepTime = tGroup.GetSleep();
					Sys.Log2(20, "{" + threadGroupThreadId + "} Thread group [" + tGroup.name + "][" + i + "] sleep [" + sleepTime + "]");
					Thread.Sleep(sleepTime);

					//get SQL query - SQL query variables have been replaced by parameters based on parameter strategy
					string sql = tGroup.GetSQLQuery(out Dictionary<string, string> dParams);
					Sys.Log2(20, "{" + threadGroupThreadId + "} Thread group [" + tGroup.name + "][" + i + "] sql [" + sql + "]");
					//get Engine for EngineClient based on Engine Strategy
					string engineName = tGroup.GetEngine();
					Sys.Log2(20, "{" + threadGroupThreadId + "} Thread group [" + tGroup.name + "][" + i + "] engine [" + engineName + "]");
					//Executy query
					Sys.Log2(10, "{" + threadGroupThreadId + "} Thread group [" + tGroup.name + "][" + i + "] prepare execute SQL on engine [" + engineName + "] with parameters " + String.Join(";", dParams.Select(x => "[$" + x.Key + "$]=[" + x.Value + "]").ToArray()));
					bool success = ExecuteQuery(engineName, sql, out long clientFromPool, out long clientToPool, out double processingTime,
						out long cachehit, out double rowfetchtime, out long matchingrowcount, out double queryNetwork, out long totalQueryTime, out string internalQueryLog, out long readCursor);
					Sys.Log("{" + threadGroupThreadId + "} Thread group [" + tGroup.name + "][" + i + "] execute SQL on engine [" + engineName + "], success [" + success.ToString() + "] query time [" + Sys.TimerGetText(totalQueryTime) + "]");

					//Store execution result in output
					if (tGroup.AddOutput(i, threadGroupThreadId, out ThreadGroupOutput tGroupOutput))
					{
						//optimize memory usage, store only values needed for output
						tGroupOutput.SetSuccess(success);
						tGroupOutput.SetInfo(engineName, dParams);
						if (conf.outputSQLQuery) tGroupOutput.SetSQL(sql);
						if (conf.outputClientTimers) tGroupOutput.SetClientTimers(clientFromPool, clientToPool);
						if (conf.outputQueryTimers) tGroupOutput.SetQueryTimers(processingTime, cachehit, rowfetchtime, matchingrowcount, totalQueryTime, readCursor);
						if (conf.outputNetworkTimers) tGroupOutput.SetNetworkTimers(queryNetwork);
						if (conf.outputIQL) tGroupOutput.SetInternalQueryLog(internalQueryLog, conf.outputIQLSearchRWA, conf.outputIQLDBQuery,  conf.outputIQLProcessorParse);
					}
				});

				tGroup.stopWatch.Stop();
				Sys.Log("----------------------------------------------------");
				Sys.Log("{" + threadGroupsThreadId + "} Thread Group [" + tGroup.name + "] stop");
				Sys.Log("----------------------------------------------------");
			});

			foreach (ThreadGroup tGroup in conf.threadGroups.Values)
			{
				if (tGroup.configLoadError) return Return.Error;
			}

			return base.OnExecute();
		}

		public override Return OnPostExecute(bool execute_ok)
		{
			if (!Directory.Exists(conf.outputFolderPath))
			{
				try
				{
					Directory.CreateDirectory(conf.outputFolderPath);
				}
				catch(Exception e)
				{
					Sys.LogError("Cannot create output directory [" + conf.outputFolderPath + "]");
					Sys.LogError(e);
					return Return.Error;
				}
			}

			string outputFileName = this.Command.Name + "_" + DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss-fff");
			string outputFilePath = Str.PathAdd(conf.outputFolderPath, outputFileName + ".csv");

			Sys.Log("Create output file [" + outputFilePath + "]");

			OutputInfo flags = OutputInfo.None;
			if (conf.outputQueryTimers) flags |= OutputInfo.QueryTimers;
			if (conf.outputQueryInfo) flags |= OutputInfo.QueryInfo;
			if (conf.outputClientTimers) flags |= OutputInfo.ClientTimers;
			if (conf.outputNetworkTimers) flags |= OutputInfo.NetworkTimers;
			if (conf.outputParameters) flags |= OutputInfo.Parameters;
			if (conf.outputSQLQuery) flags |= OutputInfo.SQLQuery;
			if (conf.outputIQLSearchRWA) flags |= OutputInfo.IQLSearchRAW;
			if (conf.outputIQLDBQuery) flags |= OutputInfo.IQLDBQuery;
			if (conf.outputIQLProcessorParse) flags |= OutputInfo.IQLQueryProcessorParse;

			bool bHeaders = false;
			using (StreamWriter sw = new StreamWriter(outputFilePath, false, Encoding.UTF8))
			{
				foreach (ThreadGroup tGroup in conf.threadGroups.Values)
				{
					tGroup.Log();
					foreach (ThreadGroupOutput tGoupOutout in tGroup.Outputs)
					{
						if (!bHeaders)
						{
							sw.WriteLine(tGoupOutout.ToCSVHeaders(flags, conf.outputCSVSeparator));
							bHeaders = true;
						}
						sw.WriteLine(tGoupOutout.ToCSV(flags, conf.outputCSVSeparator));
					}
				}
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

		private bool ExecuteQuery(string engineName, string sql, out long clientFromPool, out long clientToPool, 
			out double processingTime, out long cachehit, out double rowfetchtime, out long matchingrowcount, 
			out double queryNetwork, out long totalQueryTime, out string internalQueryLog, out long readCursor)
		{
			EngineClient _client = null;
			Engine.Client.Cursor _cursor = null;

			clientFromPool = 0;
			clientToPool = 0;
			processingTime = 0;
			cachehit = 0;
			rowfetchtime = 0;
			matchingrowcount = 0;
			queryNetwork = 0;
			totalQueryTime = 0;
			internalQueryLog = Str.Empty;
			readCursor = 0;

			Stopwatch swTotalQueryTime = new Stopwatch();
			swTotalQueryTime.Start();

			Stopwatch swClientFromPool = new Stopwatch();
			swClientFromPool.Start();
			_client = Toolbox.GetEngineClient(engineName);
			swClientFromPool.Stop();
			clientFromPool = swClientFromPool.ElapsedMilliseconds;

			if (_client == null)
			{
				swTotalQueryTime.Stop();
				totalQueryTime = swTotalQueryTime.ElapsedMilliseconds;
				return false;
			}

			try
			{
				Stopwatch swClientCursorExecute = new Stopwatch();
				swClientCursorExecute.Start();
				_cursor = _client.ExecCursor(sql);
				swClientCursorExecute.Stop();
				long execCursor = swClientCursorExecute.ElapsedMilliseconds;
				if ((_cursor != null))
				{
					Stopwatch swClientCursorRead = new Stopwatch();
					swClientCursorRead.Start();

					double.TryParse(Str.ParseToSep(_cursor.GetAttribute("processingtime"), ' '), out processingTime);
					double.TryParse(Str.ParseToSep(_cursor.GetAttribute("rowfetchtime"), ' '), out rowfetchtime);
					long.TryParse(_cursor.GetAttribute("cachehit"), out cachehit);
					long.TryParse(_cursor.GetAttribute("matchingrowcount"), out matchingrowcount);
					if (_cursor.HasAttribute("internalquerylog"))
					{
						internalQueryLog = _cursor.GetAttribute("internalquerylog");
					}
					queryNetwork = execCursor - processingTime;

					while (!_cursor.End()) _cursor.MoveNext();
					swClientCursorRead.Stop();
					readCursor = swClientCursorRead.ElapsedMilliseconds;
				}
			}
			catch (Exception ex)
			{
				Sys.LogError("Select index Cursor error : ", ex, ex.StackTrace);
				try
				{
					if (_client != null) EngineClientsPool.ToPool(_client);
				}
				catch (Exception exe)
				{
					Sys.LogError("EngineClientsPool ToPool : ", exe);
					Sys.Log(exe.StackTrace);

					swTotalQueryTime.Stop();
					totalQueryTime = swTotalQueryTime.ElapsedMilliseconds;
					return false;
				}

				swTotalQueryTime.Stop();
				totalQueryTime = swTotalQueryTime.ElapsedMilliseconds;
				return false;
			}
			finally
			{
				try
				{
					if (_cursor != null) _cursor.Close();
				}
				catch (Exception ex)
				{
					Sys.LogError("Clause cursor error : ", ex);
					Sys.Log(ex.StackTrace);
				}

				Stopwatch swClientToPool = new Stopwatch();
				swClientToPool.Start();
				Toolbox.EngineClientToPool(_client);
				swClientToPool.Stop();
				clientToPool = swClientToPool.ElapsedMilliseconds;

				swTotalQueryTime.Stop();
				totalQueryTime = swTotalQueryTime.ElapsedMilliseconds;
			}

			return true;
		}
	}

    #region enum
    public enum EngineStategy
	{
		First_available,
		Random
	}

	public enum ParameterStrategy
	{
		Ordered,
		Random
	}

	public enum SecuritySyntax
	{
		Legacy,
		Engine
	}

	[Flags]
	public enum OutputInfo
	{
		None = 0,
		QueryTimers = 1,
		QueryInfo = 2,
		ClientTimers = 4,
		NetworkTimers = 8,
		Parameters = 16,
		SQLQuery = 32,
		IQLSearchRAW = 64,
		IQLQueryProcessorParse = 128,
		IQLDBQuery = 256,
		AllIQL = IQLSearchRAW | IQLQueryProcessorParse | IQLDBQuery,
		All = QueryTimers | QueryInfo | ClientTimers | NetworkTimers | Parameters | SQLQuery | AllIQL
	}

    #endregion

    public class CmdConfigEngineBenchmark
	{
		private XDoc _XMLConf = null;

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
		public string outputFolderPath { get; private set; }
		public char outputCSVSeparator { get; private set; }
		public bool outputSQLQuery { get; private set; }
		public bool outputQueryTimers { get; private set; }
		public bool outputQueryInfo { get; private set; }
		public bool outputClientTimers { get; private set; }
		public bool outputNetworkTimers { get; private set; }
		public bool outputParameters { get; private set; }
		//output internal query 
		public bool outputIQL { get; private set; }
		public bool outputIQLSearchRWA { get; private set; }
		public bool outputIQLDBQuery { get; private set; }
		public bool outputIQLProcessorParse { get; private set; }

		public CmdConfigEngineBenchmark(XDoc conf)
		{
			_XMLConf = conf;
		}

		public bool LoadConfig()
		{
			string dataTag = Str.Empty;

			Sys.Log2(20, "----------------------------------------------------");
			Sys.Log2(20, "Load configuration");
			Sys.Log2(20, "----------------------------------------------------");

			if (_XMLConf == null)
			{
				Sys.LogError("Cannot read configuration");
				return false;
			}

            #region engines
            //engines
            dataTag = "CMD_ENGINE_LIST";
			if (!DatatagExist(dataTag)) return false;
			string engines = _XMLConf.Value(dataTag, null);
			if (String.IsNullOrEmpty(engines) || String.IsNullOrWhiteSpace(engines))
			{
				Sys.LogError("Invalid configuration property: Engines configuration - Engines cannot be empty");
				return false;
			}
			lEngines = CC.Current.Engines.CleanAliasList3(engines);
			if(lEngines == null || lEngines.Count == 0)
			{
				Sys.LogError("Invalid configuration property: Engines configuration - Resolve Engine aliases error");
				return false;
			}

			//egine stratategy
			dataTag = "CMD_ENGINE_STRATEGY";
			if (!DatatagExist(dataTag)) return false;
			string esType = _XMLConf.Value(dataTag, Str.Empty);
			if (String.IsNullOrEmpty(esType))
			{
				Sys.LogError("Invalid configuration property: Engines configuration - Engine stategy is empty");
				return false;
			}
			if (Enum.TryParse(esType, out EngineStategy est))
			{
				this.engineStategy = est;
			}
			else
			{
				Sys.LogError("Invalid configuration property: Engines configuration - Engine stategy type [" + esType + "]");
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
				Sys.LogError("Invalid configuration property: You need to create a thread group");
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
					Sys.LogError("Invalid configuration property: Thread group - name cannot be empty");
					return false;
				}

				//SQL
				string SQL = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_SQL");
				if (String.IsNullOrEmpty(SQL) || String.IsNullOrWhiteSpace(SQL))
				{
					Sys.LogError("Invalid configuration property: Thread group - SQL cannot be empty");
					return false;
				}

				//parameter files
				string paramCustomFileName = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_PARAM_CUSTON_FILE");
				if (String.IsNullOrEmpty(paramCustomFileName) || String.IsNullOrWhiteSpace(paramCustomFileName))
				{
					Sys.LogError("Invalid configuration property: Thread group - Parameter file cannot be empty");
					return false;
				}
				if(!CC.Current.FileExist("customfile", paramCustomFileName)){
					Sys.LogError("Invalid configuration property: Thread group - Parameter file, custom file [" + paramCustomFileName + "] not found");
					return false;
				}
				CCFile paramCustomFile = CC.Current.FileGet("customfile", paramCustomFileName);

				//file sep
				char fileSep = itemGridThreadGroup.ValueChar("CMD_THREAD_GROUP_GRID_PARAM_CUSTON_FILE_SEP", ';');
				if (Char.IsWhiteSpace(fileSep))
				{
					Sys.LogError("Invalid configuration property: Thread group - File separator cannot be a white space");
					return false;
				}

				//param stratategy
				string paramStrategy = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_PARAM_STRATEGY", Str.Empty);
				if (String.IsNullOrEmpty(paramStrategy))
				{
					Sys.LogError("Invalid configuration property: Thread group - Parameter stategy is empty");
					return false;
				}
				ParameterStrategy parameterStrategy;
				if (!Enum.TryParse(paramStrategy, out parameterStrategy))
				{
					Sys.LogError("Invalid configuration property: Thread group - Parameter stategy type [" + paramStrategy + "]");
					return false;
				}

				//user ACL
				bool usersACL = itemGridThreadGroup.ValueBoo("CMD_THREAD_GROUP_GRID_USERS_ACL", false);

				//Threads number
				int threadNumber = itemGridThreadGroup.ValueInt("CMD_THREAD_GROUP_GRID_THREADS_NUMBER", 5);
				if (threadNumber <= 0)
				{
					Sys.LogError("Invalid configuration property: Thread group - Thread number must be > 0");
					return false;
				}

				//Threads sleep
				int threadSleepMin;
				int threadSleepMax;
				string threadSleep = itemGridThreadGroup.Value("CMD_THREAD_GROUP_GRID_THREADS_SLEEP", "3;10");
				if (String.IsNullOrEmpty(threadSleep) || String.IsNullOrWhiteSpace(threadSleep))
				{
					Sys.LogError("Invalid configuration property: Thread group - Thread sleep is empty");
					return false;
				}
				string sleepMin = Str.ParseToSep(threadSleep, ';');
				string sleepMax = Str.ParseFromSep(threadSleep, ';');
				if (!int.TryParse(sleepMin, out threadSleepMin) || !int.TryParse(sleepMax, out threadSleepMax))
				{
					Sys.LogError("Invalid configuration property: Thread group - Thread sleep format must be <min;max> where min and max integers");
					return false;
				}
				if (threadSleepMin > threadSleepMax)
				{
					Sys.LogError("Invalid configuration property: Thread group - Thread sleep min > Thread sleep max");
					return false;
				}

				//Max execution time
				int maxExecutionTime = itemGridThreadGroup.ValueInt("CMD_THREAD_GROUP_GRID_MAX_TIME", 60);
				if (threadNumber <= 0)
				{
					Sys.LogError("Invalid configuration property: Thread group -  Execution time must be > 0");
					return false;
				}

				//Max iteration
				int maxIterations = itemGridThreadGroup.ValueInt("CMD_THREAD_GROUP_GRID_MAX_ITERATION", 100);
				if (threadNumber <= 0)
				{
					Sys.LogError("Invalid configuration property: Thread group - Max iteration must be > 0");
					return false;
				}

				ThreadGroup tg = new ThreadGroup(name, SQL, paramCustomFile, fileSep, parameterStrategy, usersACL, threadNumber,
					threadSleepMin, threadSleepMax, maxExecutionTime, maxIterations);
				threadGroups.Add(name, tg);
			}
			#endregion

			#region user ACL
			//load users only if a thread group use UserAcls
			if(threadGroups.Where(x => x.Value.addUserACLs == true).ToList().Count > 0)
			{
				//security syntax
				dataTag = "CMD_SECURITY_SYNTAX";
				if (!DatatagExist(dataTag)) return false;
				string ssType = _XMLConf.Value(dataTag, Str.Empty);
				if (String.IsNullOrEmpty(ssType))
				{
					Sys.LogError("Invalid configuration property: User ACLs - Security syntax is empty");
					return false;
				}
				if (Enum.TryParse(ssType, out SecuritySyntax ss))
				{
					this.securitySyntax = ss;
				}
				else
				{
					Sys.LogError("Invalid configuration property: User ACLs - Security syntax [" + ssType + "]");
					return false;
				}
				//Engine security only available >= 11.3.0
				if(this.securitySyntax == SecuritySyntax.Engine && (Sys.GetMajorVersion() < 11 || Sys.GetMinorVersion() < 3))
				{
					Sys.LogError("Invalid configuration property: User ACLs - Security syntax, Engine security requires >= 11.3.0, current version [" + Sys.GetVersion() +"] ");
					return false;
				}

				//domain
				dataTag = "CMD_USERS_DOMAIN";
				if (!DatatagExist(dataTag)) return false;
				string securityDomain = _XMLConf.Value(dataTag, null);
				if (String.IsNullOrEmpty(engines) || String.IsNullOrWhiteSpace(engines))
				{
					Sys.LogError("Invalid configuration property: User ACLs - Security domain cannot be empty");
					return false;
				}
				if (!CC.Current.Domains.Exist(securityDomain))
				{
					Sys.LogError("Invalid configuration property: User ACLs - Security domain not found [" + securityDomain + "]");
					return false;
				}
				domain = CC.Current.Domains.Get(securityDomain);

				//users grid
				dataTag = "CMD_USERS_GRID";
				if (!DatatagExist(dataTag, false))
				{
					Sys.LogError("Invalid configuration property: User ACLs - user list is empty");
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
						Sys.LogError("Invalid configuration property: User ACLs - user is empty");
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
			//output file path
			dataTag = "CMD_OUTPUT_FOLDER_PATH";
			if (!DatatagExist(dataTag)) return false;
			outputFolderPath = _XMLConf.Value(dataTag, null);
			if (String.IsNullOrEmpty(outputFolderPath) || String.IsNullOrWhiteSpace(outputFolderPath))
			{
				Sys.LogError("Invalid configuration property: Output - Folder path cannot be empty");
				return false;
			}

			//CSV separator
			dataTag = "CMD_OUTPUT_CSV_SEPARATOR";
			if (!DatatagExist(dataTag)) return false;
			outputCSVSeparator = _XMLConf.ValueChar(dataTag, '\t');

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
			dataTag = "CMD_OUTPUT_NETWORK_TIMERS";
			if (!DatatagExist(dataTag)) return false;
			outputNetworkTimers = _XMLConf.ValueBoo(dataTag, true);

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
			dataTag = "CMD_OUTPUT_INTERNALQUERYLOG_QUERY_PROCESSOR_PARSE";
			if (!DatatagExist(dataTag)) return false;
			outputIQLProcessorParse = _XMLConf.ValueBoo(dataTag, false);

			//if any outputInternalQueryLog*, outputInternalQueryLog = true
			if (outputIQLSearchRWA || outputIQLProcessorParse || outputIQLDBQuery)
			{
				outputIQL = true;
			}
			#endregion

			Sys.Log2(20, "Load configuration OK");

			LogConfig();

			return true;
		}

        public void LogConfig()
        {
            Sys.Log("----------------------------------------------------");
            Sys.Log("Engines : [" + String.Join(",", this.lEngines.Select(x => x.DisplayName).ToArray()) + "]");
			Sys.Log("Engine strategy : [" + Enum.GetName(typeof(EngineStategy), this.engineStategy) + "]");
			Sys.Log("----------------------------------------------------");
			Sys.Log("Execute thread groups in parallel : [" + this.threadGroupsInParallel.ToString() + "]");
			foreach (string tgName in threadGroups.Keys)
			{
				threadGroups.TryGetValue(tgName, out ThreadGroup tg);
				Sys.Log("Thread group [" + tgName + "] configuration:");
				Sys.Log(tg.ToString());
				Sys.Log("----------------------------------------------------");
			}
			if (domain != null && lUsers != null && lUsers.Count > 0)
			{
				Sys.Log("Security syntax : [" + Enum.GetName(typeof(SecuritySyntax), this.securitySyntax) + "]");
				Sys.Log("Security domain [" + domain.Name + "]");
				Sys.Log("Users [" + lUsers.ToStr(';') + "]");
				Sys.Log("----------------------------------------------------");
			}
			Sys.Log("Output folder path : [" + this.outputFolderPath + "]");
			Sys.Log("Output CSV separator : [" + this.outputCSVSeparator + "]");
			Sys.Log("Output query timers : [" + this.outputSQLQuery.ToString() + "]");
			Sys.Log("Output query timers : [" + this.outputQueryTimers.ToString() + "]");
			Sys.Log("Output query info : [" + this.outputQueryInfo.ToString() + "]");
			Sys.Log("Output client timers : [" + this.outputClientTimers.ToString() + "]");
			Sys.Log("Output network timers : [" + this.outputNetworkTimers.ToString() + "]");
			Sys.Log("Output parameters : [" + this.outputParameters.ToString() + "]");
			Sys.Log("Output internal query log - search RWA : [" + this.outputIQLSearchRWA.ToString() + "]");
			Sys.Log("Output internal query log - DB Query : [" + this.outputIQLDBQuery.ToString() + "]");
			Sys.Log("Output internal query log - query processor parse : [" + this.outputIQLProcessorParse.ToString() + "]");
			Sys.Log("----------------------------------------------------");
		}

        private bool DatatagExist(string dataTag, bool logError = true)
        {
            if (!_XMLConf.EltExist(dataTag))
            {
                if (logError) Sys.LogError("Invalid configuration property, datatag [", dataTag, "] is missing");
                return false;
            }
            return true;
        }
    }

	public class ThreadGroup
	{
		private CmdConfigEngineBenchmark _conf;
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

		public int nbIterration = 0;
		public Stopwatch stopWatch = new Stopwatch();

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
			this.maxExecutionTime = maxExecutionTime * 1000;	//seconds to milliseconds
			this.maxIteration = maxIteration;
		}

		public List<ThreadGroupOutput> Outputs
		{
			get
			{
				return _dOUtput.Values.ToList();
			}
		}

		public override string ToString()
		{
			StringBuilder sb = new StringBuilder();
			sb.AppendLine();
			sb.AppendLine("Name = [" + this.name + "]");
			sb.AppendLine("SQL = [" + this.sql + "]");
			sb.AppendLine("Custom File = [" + this.paramCustomFile.Name + "]");
			sb.AppendLine("File separator = [" + this.fileSep + "]");
			sb.AppendLine("Parameter strategy : [" + Enum.GetName(typeof(ParameterStrategy), this.paramStrategy) + "]");
			sb.AppendLine("User ACL = [" + this.addUserACLs.ToString() + "]");
			sb.AppendLine("Thread Number = [" + this.threadNumber + "]");
			sb.AppendLine("Thread Sleep Min = [" + this.threadSleepMin + "]");
			sb.AppendLine("Thread Sleep Max = [" + this.threadSleepMax + "]");
			sb.AppendLine("Thread Number = [" + this.threadNumber + "]");
			sb.AppendLine("Max Execution Time = [" + this.maxExecutionTime + "] ms");
			sb.AppendLine("Max Iteration = [" + this.maxIteration + "]");

			return sb.ToString();
		}

		public bool Init(CmdConfigEngineBenchmark conf)
		{
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
					Sys.LogWarning("Line [" + realLineNumber + "] is empty in parameter file [" + paramCustomFile.Name + "], line is ignored");
					i++;
					continue;
				}

				if (i == 0)
				{
					int duplicates = lineColumns.GroupBy(x => x).Where(g => g.Count() > 1).Count();
					if(duplicates > 0)
					{
						Sys.LogError("Duplicates headers in parameter file [" + paramCustomFile.Name + "]");
						return false;
					}
					headers = lineColumns;
				}
				else
				{
					if(lineColumns.Count != headers.Count)
					{
						Sys.LogError("Line [" + realLineNumber + "] does not have the same number of columns as header in parameter file [" + paramCustomFile.Name + "]");
						return false;
					}

					Dictionary<string, string> d = new Dictionary<string, string>();
					int j = 0;
					foreach (string param in lineColumns) { 
						if (String.IsNullOrEmpty(param))
						{
							Sys.LogWarning("Line [" + realLineNumber + "] contains an empty parameter for column [" + headers[j] + "] in parameter file [" + paramCustomFile.Name + "]");
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
			if(engineStategy == EngineStategy.First_available)
			{
				foreach(CCEngine engine in lEngines)
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

			//add internalquerylog
			if (_conf.outputIQL)
			{
				if (!sqlWithParams.Contains("internalquerylog"))
				{
					sqlWithParams = Str.Replace(sqlWithParams, "select", "select internalquerylog,");
				}
			}

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
			if(this.paramStrategy == ParameterStrategy.Random)
			{
				lock (syncLock)
				{
					int index = _rand.Next(0, _parameters.Count);
					d = new Dictionary<string, string>(_parameters.ElementAt(index));
				}
			}
			return d;
		}

		public bool AddOutput(int id, int threadId, out ThreadGroupOutput tGroupOUtput)
		{
			tGroupOUtput = new ThreadGroupOutput(this.name, id, threadId);
			if (!_dOUtput.TryAdd(id, tGroupOUtput))
			{
				tGroupOUtput = null;
				return false;
			}
			return true;
		}

		public int GetOutputCount()
		{
			return _dOUtput.Count();
		}

		public double GetOutputAVGProcessingTime()
		{
			return _dOUtput.Average(x => x.Value.processingTime);
		}

		public double GetOutputAVGClientTime(string fromTo)
		{
			if (Str.EQNC(fromTo, "from")) return _dOUtput.Average(x => x.Value.clientFromPool);
			if (Str.EQNC(fromTo, "to")) return _dOUtput.Average(x => x.Value.clientToPool);
			return 0;
		}

		public double GetOutputAVGQueryNetworkTime()
		{
			return _dOUtput.Average(x => x.Value.queryNetwork);
		}

		public double GetOutputAVGRowFetchTime()
		{
			return _dOUtput.Average(x => x.Value.rowfetchtime);
		}

		public double GetOutputMAXProcessingTime()
		{
			return _dOUtput.Max(x => x.Value.processingTime);
		}

		public double GetOutputSuccessByStatusCount(bool successStatus)
		{
			return _dOUtput.Where(x => x.Value.success == successStatus).Count();
		}

		public double GetOutputPercentileProcessingTime(double percentil)
		{
			List<double> l = _dOUtput.OrderBy(x => x.Value.processingTime).Select(x => x.Value.processingTime).ToList();
			int index = (int)Math.Ceiling(percentil * l.Count);
			if (index >= l.Count) return _dOUtput.Max(x => x.Value.processingTime);
			return l.ElementAt(index);
		}

		public double GetQPS()
		{
			return ((double)(GetOutputCount()) / ((double)(stopWatch.ElapsedMilliseconds) / 1000));
		}

		public void Log()
		{
			Sys.Log("----------------------------------------------------");
			Sys.Log("Thread Group [" + name + "]");
			Sys.Log("----------------------------------------------------");
			Sys.Log("Execution time [" + Sys.TimerGetText(stopWatch.ElapsedMilliseconds) + "]");
			Sys.Log("Number of iterations [" + GetOutputCount() + "]");
			Sys.Log("Number of success queries [" + GetOutputSuccessByStatusCount(true) + "]");
			Sys.Log("Number of failed queries [" + GetOutputSuccessByStatusCount(false) + "]");
			Sys.Log("----------------------------------------------------");
			Sys.Log("Average processing time [" + Sys.TimerGetText(GetOutputAVGProcessingTime()) + "]");
			Sys.Log("Max processing time [" + Sys.TimerGetText(GetOutputMAXProcessingTime()) + "]");
			Sys.Log("Average network time [" + Sys.TimerGetText(GetOutputAVGQueryNetworkTime()) + "]");
			Sys.Log("Average row fetch time [" + Sys.TimerGetText(GetOutputAVGRowFetchTime()) + "]");
			Sys.Log("Average engine client from pool time [" + Sys.TimerGetText(GetOutputAVGClientTime("from")) + "]");
			Sys.Log("Average engine client to pool time [" + Sys.TimerGetText(GetOutputAVGClientTime("to")) + "]");
			Sys.Log("----------------------------------------------------");
			Sys.Log("25th percentile processing time [" + Sys.TimerGetText(GetOutputPercentileProcessingTime(0.25)) + "]");
			Sys.Log("50th percentile processing time [" + Sys.TimerGetText(GetOutputPercentileProcessingTime(0.5)) + "]");
			Sys.Log("75th percentile processing time [" + Sys.TimerGetText(GetOutputPercentileProcessingTime(0.75)) + "]");
			Sys.Log("80th percentile processing time [" + Sys.TimerGetText(GetOutputPercentileProcessingTime(0.8)) + "]");
			Sys.Log("85th percentile processing time [" + Sys.TimerGetText(GetOutputPercentileProcessingTime(0.85)) + "]");
			Sys.Log("90th percentile processing time [" + Sys.TimerGetText(GetOutputPercentileProcessingTime(0.9)) + "]");
			Sys.Log("95th percentile processing time [" + Sys.TimerGetText(GetOutputPercentileProcessingTime(0.95)) + "]");
			Sys.Log("----------------------------------------------------");
			Sys.Log("QPS (Query Per Second) [" + GetQPS().ToString("0.###") + "] ");
			Sys.Log("----------------------------------------------------");
		}
	}

	public class ThreadGroupOutput
	{
		public string threadGroupName { get; private set; }
		public int id { get; private set; }
		//info
		public string sql { get; private set; }
		public int threadId { get; private set; }
		public bool success { get; private set; }
		public Dictionary<string, string> dParams { get; private set; }
		public string engineName { get; private set; }
		
		//client timers
		public long clientFromPool { get; private set; }
		public long clientToPool { get; private set; }
		
		//query timers
		public double processingTime { get; private set; }
		public long cachehit { get; private set; }
		public double rowfetchtime { get; private set; }
		public long matchingrowcount { get; private set; }
		public long totalQueryTime { get; private set; }
		public long readCursor { get; private set; }

		//network
		public double queryNetwork { get; private set; }

		//internal query log
		//static list to store indexes names
		private static ConcurrentBag<string> _IQLIndexes = new ConcurrentBag<string>();
		//search RWA (per index)
		public Dictionary<string, double> IQLSearchRAW { get; private set; }  = new Dictionary<string, double>();
		//Execute DBQuery (per index)
		public Dictionary<string, double> IQLExecuteDBQuery { get; private set; } = new Dictionary<string, double>();
		//Fetching DBQuery (per index)
		public Dictionary<string, double> IQLFetchingDBQuery { get; private set; } = new Dictionary<string, double>();
		//query processor (per query)
		public double internalQueryLogQueryProcessorParse { get; private set; }


		public ThreadGroupOutput(string threadGroupName, int id, int threadId)
		{
			this.threadGroupName = threadGroupName;
			this.id = id;
			this.threadId = threadId;
		}

		public void SetSuccess(bool success)
		{
			this.success = success;
		}

		public void SetSQL(string sql)
		{
			this.sql = sql;
		}

		public void SetInfo(string engineName, Dictionary<string, string> dParams)
		{
			this.engineName = engineName;
			this.dParams = dParams;
		}

		public void SetClientTimers(long clientFromPool, long clientToPool)
		{
			this.clientFromPool = clientFromPool;
			this.clientToPool = clientToPool;
		}

		public void SetQueryTimers(double processingTime, long cachehit, double rowfetchtime, long matchingrowcount, long totalQueryTime, long readCursor)
		{
			this.processingTime = processingTime;
			this.cachehit = cachehit;
			this.rowfetchtime = rowfetchtime;
			this.matchingrowcount = matchingrowcount;
			this.totalQueryTime = totalQueryTime;
			this.readCursor = readCursor;
		}

		public void SetNetworkTimers(double queryNetwork)
		{
			this.queryNetwork = queryNetwork;
		}

		public void SetInternalQueryLog(string intquerylog, bool searchRWA, bool DBQuery, bool queryProcessorParse)
		{
			
			if (String.IsNullOrEmpty(intquerylog)) return;
			XmlDocument internalQueryLog = new XmlDocument();
			internalQueryLog.LoadXml(intquerylog);

			if (queryProcessorParse)
			{
				//<timing name="QueryProcessor::Parse" duration="4.32 ms" start="0.14 ms" tid="5" />
				XmlNode nodeTimingQueryProcessorParse = internalQueryLog.SelectSingleNode("//timing[@name='QueryProcessor::Parse']");
				if (nodeTimingQueryProcessorParse != null)
				{
					XmlAttribute attrQueryProcessorParseDuration = (XmlAttribute)nodeTimingQueryProcessorParse.Attributes.GetNamedItem("duration");
					if(double.TryParse(Str.ParseToSep(attrQueryProcessorParseDuration.Value, ' '), out double d)){
						internalQueryLogQueryProcessorParse = d;
					}
				}
			}

			//indexes
			if (searchRWA || DBQuery)
			{
				//loop on <IndexSearch> nodes
				XmlNodeList indexeNodes = internalQueryLog.SelectNodes("//IndexSearch");
				if (indexeNodes != null)
				{
					foreach (XmlNode indexNode in indexeNodes)
					{
						double duration = 0;
						//get index name from index attribute: <IndexSearch index="index_name">
						XmlAttribute attrIndexName = (XmlAttribute)indexNode.Attributes.GetNamedItem("index");
						string indexName = attrIndexName.Value;

						if (!_IQLIndexes.Contains(indexName)) _IQLIndexes.Add(indexName);

						if (searchRWA)
						{
							//get SearchRWA duration
							//<timing name="SearchRWA" duration="4.39 ms" start="6.78 ms" tid="22" />
							if (GetIndexDuration(indexNode, "timing[@name='SearchRWA']", out duration))
								IQLSearchRAW.Add(indexName, duration);
						}

						if (DBQuery)
						{
							//get ExecuteDBQuery & Fetching DBQuery duration
							//<timing name="ExecuteDBQuery" duration="15.56 ms" start="68.74 ms" tid="22" />
							if (GetIndexDuration(indexNode, "timing[@name='ExecuteDBQuery']", out duration))
								IQLExecuteDBQuery.Add(indexName, duration);
							else IQLExecuteDBQuery.Add(indexName, 0);	//set duration to 0 (if ExecuteDBQuery timing tag does not exist in the InternalQueryLog it means the query is cached by the Engine and does not need to be evaluated again)
							//<timing name="Fetching DBQuery" duration="15.62 ms" start="68.69 ms" tid="22" />
							if (GetIndexDuration(indexNode, "timing[@name='Fetching DBQuery']", out duration))
								IQLFetchingDBQuery.Add(indexName, duration);
						}
					}
				}
			}
			
		}

		private bool GetIndexDuration(XmlNode indexNode, string xPath, out double duration)
		{
			duration = 0; 
			XmlNode node = indexNode.SelectSingleNode(xPath);
			if (node == null) return false;
			XmlAttribute attrSearchRWADuration = (XmlAttribute)node.Attributes.GetNamedItem("duration");
			if (attrSearchRWADuration == null) return false;
			return double.TryParse(Str.ParseToSep(attrSearchRWADuration.Value, ' '), out duration);			
		}
		
		private string GetInternalQueryLogCSVHeaders(OutputInfo flags, char separator = ';')
		{
			ListStr lHeaders = new ListStr();
			if ((flags & OutputInfo.IQLSearchRAW) == OutputInfo.IQLSearchRAW)
			{
				lHeaders.Add(_IQLIndexes.Select(x => x + " [SearchRWA]").ToArray());
			}
			if ((flags & OutputInfo.IQLDBQuery) == OutputInfo.IQLDBQuery)
			{
				lHeaders.Add(_IQLIndexes.Select(x => x + " [ExecuteDBQuery]").ToArray());
				lHeaders.Add(_IQLIndexes.Select(x => x + " [FetchingDBQuery]").ToArray());
			}
			if ((flags & OutputInfo.IQLQueryProcessorParse) == OutputInfo.IQLQueryProcessorParse)
			{
				lHeaders.Add("QueryProcessorParse");
			}
			return lHeaders.ToStr(separator);
		}

		private string GetInternalQueryLogCSV(OutputInfo flags, char separator = ';')
		{
			ListStr lDurations = new ListStr();
			if ((flags & OutputInfo.IQLSearchRAW) == OutputInfo.IQLSearchRAW) lDurations.Add(GetInternalQueryLogIndexDurations(IQLSearchRAW));
			if ((flags & OutputInfo.IQLDBQuery) == OutputInfo.IQLDBQuery) lDurations.Add(GetInternalQueryLogIndexDurations(IQLExecuteDBQuery));
			if ((flags & OutputInfo.IQLDBQuery) == OutputInfo.IQLDBQuery) lDurations.Add(GetInternalQueryLogIndexDurations(IQLFetchingDBQuery));
			if ((flags & OutputInfo.IQLQueryProcessorParse) == OutputInfo.IQLQueryProcessorParse)
			{
				lDurations.Add(internalQueryLogQueryProcessorParse.ToString());
			}
			return lDurations.ToStr(separator);
		}

		private ListStr GetInternalQueryLogIndexDurations(Dictionary<string, double> d)
		{
			ListStr lDurations = new ListStr();
			foreach (string indexName in _IQLIndexes)
			{
				if (d.ContainsKey(indexName))
				{
					if (d.TryGetValue(indexName, out double duration))
					{
						lDurations.Add(duration.ToString());
					}
				}
				else lDurations.Add("");
			}
			return lDurations;
		}

		public string ToCSVHeaders(OutputInfo flags, char separator = ';')
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("thread group name" + separator);
			sb.Append("iteration" + separator);
			sb.Append("success" + separator);
			sb.Append("engine name" + separator);
			if ((flags & OutputInfo.SQLQuery) == OutputInfo.SQLQuery)
			{ 
				sb.Append("sql" + separator);
			}
			if ((flags & OutputInfo.QueryTimers) == OutputInfo.QueryTimers)
			{
				sb.Append("totalQueryTime (ms)" + separator);
				sb.Append("processingTime (ms)" + separator);
				sb.Append("rowfetchtime (ms)" + separator);
				sb.Append("readCursor (ms)" + separator);
			}
			if ((flags & OutputInfo.QueryInfo) == OutputInfo.QueryInfo)
			{
				sb.Append("cachehit" + separator);
				sb.Append("matchingrowcount" + separator);
			}
			if ((flags & OutputInfo.ClientTimers) == OutputInfo.ClientTimers)
			{
				sb.Append("clientFromPool (ms)" + separator);
				sb.Append("clientToPool (ms)" + separator);
			}
			if ((flags & OutputInfo.NetworkTimers) == OutputInfo.NetworkTimers)
			{
				sb.Append("queryNetwork (ms)" + separator);
			}
			if ((flags & OutputInfo.Parameters) == OutputInfo.Parameters)
			{
				sb.Append(String.Join(separator.ToString(), dParams.Select(x => "$" + x.Key + "$").ToArray()) + separator);
			}
			if ((flags & OutputInfo.IQLSearchRAW) == OutputInfo.IQLSearchRAW || (flags & OutputInfo.IQLQueryProcessorParse) == OutputInfo.IQLQueryProcessorParse || (flags & OutputInfo.IQLDBQuery) == OutputInfo.IQLDBQuery)
			{
				sb.Append(GetInternalQueryLogCSVHeaders(flags, separator) + separator);
			}
			sb.Remove(sb.Length - 1, 1);
			return sb.ToString();
		}

		public string ToCSV(OutputInfo flags, char separator = ';')
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(threadGroupName.ToString() + separator);
			sb.Append(id.ToString() + separator);
			sb.Append(success.ToString() + separator);
			sb.Append(engineName.ToString() + separator);
			if ((flags & OutputInfo.SQLQuery) == OutputInfo.SQLQuery)
			{
				sb.Append(sql.ToString() + separator);
			}
			if ((flags & OutputInfo.QueryTimers) == OutputInfo.QueryTimers)
			{
				sb.Append(totalQueryTime.ToString() + separator);
				sb.Append(processingTime.ToString() + separator);
				sb.Append(rowfetchtime.ToString() + separator);
				sb.Append(readCursor.ToString() + separator);
			}
			if ((flags & OutputInfo.QueryInfo) == OutputInfo.QueryInfo)
			{
				sb.Append(cachehit.ToString() + separator);
				sb.Append(matchingrowcount.ToString() + separator);
			}
			if ((flags & OutputInfo.ClientTimers) == OutputInfo.ClientTimers)
			{
				sb.Append(clientFromPool.ToString() + separator);
				sb.Append(clientToPool.ToString() + separator);
			}
			if ((flags & OutputInfo.NetworkTimers) == OutputInfo.NetworkTimers)
			{
				sb.Append(queryNetwork.ToString() + separator);
			}
			if ((flags & OutputInfo.Parameters) == OutputInfo.Parameters)
			{
				sb.Append(String.Join(separator.ToString(), dParams.Select(x => x.Value).ToArray()) + separator);
			}
			if ((flags & OutputInfo.IQLSearchRAW) == OutputInfo.IQLSearchRAW || (flags & OutputInfo.IQLQueryProcessorParse) == OutputInfo.IQLQueryProcessorParse || (flags & OutputInfo.IQLDBQuery) == OutputInfo.IQLDBQuery)
			{
				sb.Append(GetInternalQueryLogCSV(flags, separator) + separator);
			}
			sb.Remove(sb.Length - 1, 1);
			return sb.ToString();
		}

	}

	public static class Toolbox
	{
		//Get engine clients
		//please make sure client is return to the pool before the end of execution.
		public static EngineClient GetEngineClient(string engineName)
		{
			EngineClient client = null;
			try
			{
				client = EngineClientsPool.FromPool(engineName);
			}
			catch (Exception ex)
			{
				Sys.LogError("Cannot get EngineClient for engine [", engineName, "]");
				Sys.LogError(ex);

				//exit execution, return client to pool 
				EngineClientToPool(client);
				return null;
			}
			return client;
		}

		//return a client to the pool
		public static bool EngineClientToPool(EngineClient client)
		{
			try
			{
				EngineClientsPool.ToPool(client);
			}
			catch (Exception exe)
			{
				Sys.LogError("Cannot return EngineClient to pool for [", client.Name, "]");
				Sys.LogError(exe);
				return false;
			}
			return true;
		}

		//return engine status
		public static EngineCustomStatus GetEngineStatus(string engineName)
		{
			//try to open connexion to the engine
			EngineClient _client = GetEngineClient(engineName);
			if (_client == null) return null;

			//check engine is alive
			EngineCustomStatus ECS = new EngineCustomStatus(_client);

			//return client to pool
			EngineClientToPool(_client);
			return ECS;
		}

		public static bool GetUserRightsAsSqlStr(string userNameOrId, string domainName, SecuritySyntax securitySyntax, out CCPrincipal principal, out string userRights)
		{
			userRights = null;

			principal = CC.Current.GetPrincipalAny(userNameOrId, domainName);
			if (principal == null)
			{
				Sys.LogError("Cannot load user with Id or Name [" + userNameOrId + "] in domain [" + domainName + "]");
				return false;
			}

			long pid = CC.Current.NativeDomains.GetPrincipalByUserId(principal.UserId);
			if (pid == 0)
			{
				Sys.LogError("Cannot get principal ID from native domains , user name [" + principal.Name + "]");
				return false;
			}
			
			if (securitySyntax == SecuritySyntax.Engine)
			{
				userRights = RightsAsSqlStrXb(principal.UserId, new ListStr(), new ListStr());
			}
			else if(securitySyntax == SecuritySyntax.Legacy)
			{
				int flags = NativeDomains.UR_USE_CACHE | NativeDomains.UR_DO_USER_LSTSTR | NativeDomains.UR_DO_USER_SQLSTR;

				if (!CC.Current.NativeDomains.CalculateUserRightsWithCache(pid, null, null, flags, out ListStr user_list,
					out ListStr field_list, out string user_rights_sql, out string xfield_parts, out string fingerprint))
				{
					Sys.LogError("Cannot calculate user rights from native domains for user name [" + principal.Name + "]");
					return false;
				}

				userRights = RightsAsSqlStr(user_rights_sql);
			}

			if (userRights == null)
			{
				Sys.LogError("Cannot build SQL rights for user name [" + principal.Name + "]");
				return false;
			}

			return true;
		}

		// es-4959 - new syntax for security clause -- called iff (Sys.SecurityInEngine() == true) --
		// _RightsAsSqlStrXb ::= CHECKACLS('AccessLists="accesslist1,accesslist2,...", DeniedLists="deniedlist1,..."{%F%}') FOR('identity1',...) OPTIONAL('opt_identity1',...) VIRTUAL('virt_identity1',...)
		// where {%F%} is the placeholder for ",FieldRightsAsTextPartWeights="true""
		// when Query.RespectFieldPermissions or Session.Profile.RespectFieldPermissions are true
		private static string RightsAsSqlStrXb(string userId, ListStr otherIdentities, ListStr otherVirtualIdentities)
		{
			if (Str.IsEmpty(userId)) return null;
			int deniedcount = CC.Current.Global.DeniedListCount;
			int accesscount = CC.Current.Global.AccessListCount;
			if (accesscount + deniedcount == 0) return null;

			StringBuilder sb = new StringBuilder();
			sb.Append("CHECKACLS('");
			if (accesscount > 0)
			{
				sb.Append("accesslists=\"");
				// accesslist1,accesslist2...
				for (int accessindex = 1; accessindex <= accesscount; accessindex++)
				{
					if (accessindex > 1) sb.Append(',');
					sb.Append("accesslist"); sb.Append(Sys.ToStr(accessindex));
				}
				sb.Append('"');
			}

			if (deniedcount > 0)
			{
				if (accesscount > 0) sb.Append(',');
				sb.Append("deniedlists=\"");
				// deniedlist1,deniedlist2...
				for (int deniedindex = 1; deniedindex <= deniedcount; deniedindex++)
				{
					if (deniedindex > 1) sb.Append(',');
					sb.Append("deniedlist"); sb.Append(Sys.ToStr(deniedindex));
				}
				sb.Append('"');
			}

			//sb.Append("{%F%}"); // either ",FieldRightsAsTextPartWeights=\"true\"" or ""

			// add userId.... (required) ; es-5480 - quote ids with Str.SqlValue()
			sb.Append("') FOR ("); sb.Append(Str.SqlValue(userId)); sb.Append(")");

			// add other identities (optional) ....
			int nOptional = ListStr.GetCount(otherIdentities);
			if (nOptional > 0)
			{
				sb.Append(" OPTIONAL("); sb.Append(Str.SqlValue(otherIdentities)); sb.Append(')');
			}

			// add virtualIdentities ...
			int nVirtual = otherVirtualIdentities?.Count ?? 0;
			if (nVirtual > 0)
			{
				sb.Append(" VIRTUAL("); sb.Append(Str.SqlValue(otherVirtualIdentities)); sb.Append(')');
			}

			return sb.ToString();
		}

		// ES-4537 : optimize _SqlRights
		// @param userListSql : sql string list of user rights ids ::= 'dom|id1'[,'dom|idN']*
		// @returns denied lists and access lists as SQL String  (or null if no rights or access/denied lists count == 0)
		// rightsAsSqlStr ::= {accesslists} | {deniedlists} and {accesslists}  | {deniedlists}
		private static string RightsAsSqlStr(string userListSql)
		{
			if (Str.IsEmpty(userListSql)) return null;
			int deniedcount = CC.Current.Global.DeniedListCount;
			int accesscount = CC.Current.Global.AccessListCount;
			if (accesscount + deniedcount == 0) return null;

			StringBuilder sb = new StringBuilder();

			//deniedlists ::=   (not(deniedlist1 in (ids*))) [ and (not(deniedlistN in (ids*)))]*
			string deniedlistN;
			for (int deniedindex = 1; deniedindex <= deniedcount; deniedindex++)
			{
				deniedlistN = Str.And("deniedlist", deniedindex);
				if (deniedindex != 1) Str.Add(sb, " and ");
				Str.Add(sb, "(not(", deniedlistN, " in (", userListSql, ")))");
			}
			if ((deniedcount > 0) && (accesscount > 0))
			{
				Str.Add(sb, " and ");
			}
			//accesslists ::= (accesslist1 is null or accesslist1 in (ids*)) [ and (accesslist1 is null or accesslist1 in (ids*))]*
			string accesslistN;
			for (int accessindex = 1; accessindex <= accesscount; accessindex++)
			{
				accesslistN = Str.And("accesslist", accessindex);
				if (accessindex != 1) Str.Add(sb, " and ");
				Str.Add(sb, "(", accesslistN, " is null or ", accesslistN, " in (", userListSql, "))");
			}

			return sb.ToString();
		}
	}

	public class EngineCustomStatus
	{
		public bool IsAlive { get; } = false;
		public int ConnectionCount { get; } = 0;
		public string Host { get; } = Str.Empty;
		public string Name { get; } = Str.Empty;
		public int Port { get; } = 0;
		public DateTime StartTime { get; } = new DateTime();
		public string Version { get; } = Str.Empty;
		public long VMSize { get; } = 0;

		public EngineCustomStatus(EngineClient client)
		{
			if (client == null) return;

			if (client.IsAlive())   //engine is alive, get status
			{
				this.IsAlive = true;
				EngineStatus status = client.GetEngineStatus();
				this.ConnectionCount = status.ConnectionCount;
				this.StartTime = status.StartTime;
				this.Version = status.Version;
				this.VMSize = status.VMSize;
				this.Host = client.Host;
				this.Name = client.Name;
				this.Port = client.Port;
			}
		}

		public string GetDisplayIPPort()    //IP:PORT
		{
			return Host + ":" + Port;
		}

		public string GetDisplayStartTime() //MM/DD/YYYY 12:26 PM
		{
			return StartTime.ToShortDateString() + " " + StartTime.ToLongDateString();
		}

		public string GetDisplayVMSize()    //human readable
		{
			return Str.Size(VMSize);
		}
	}
}
