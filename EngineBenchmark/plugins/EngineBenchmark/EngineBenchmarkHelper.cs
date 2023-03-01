using System;
using System.Text;
using Sinequa.Common;
using Sinequa.Configuration;
using Sinequa.Search;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using System.Xml.Linq;
using Tensorflow.Tfprof.Pprof;

namespace Sinequa.Plugin
{

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

	public enum SecurityInput
	{
		Table,
		File
	}

	public static class Toolbox
	{
		//Get engine client
		//please make sure client is return to the pool
		public static EngineClient EngineClientFromPool(string engineName, string message = null)
		{
			EngineClient client = null;
			try
			{
				if (!String.IsNullOrEmpty(message)) Sys.Log($"Engine [{engineName}] open session - {message}");
				client = EngineClientsPool.FromPool(engineName);
			}
			catch (Exception ex)
			{
				Sys.LogError($"Cannot get EngineClient for engine [{engineName}]");
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
				Sys.LogError($"Cannot return EngineClient to pool for [{client.Name}]");
				Sys.LogError(exe);
				return false;
			}
			return true;
		}

		//return engine status
		public static EngineCustomStatus GetEngineStatus(string engineName)
		{
			//try to open connexion to the engine
			EngineClient client = EngineClientFromPool(engineName, $"GetEngineStatus");
			if (client == null) return null;

			//check engine is alive
			EngineCustomStatus ECS = new EngineCustomStatus(client);

			//return client to pool
			EngineClientToPool(client);
			return ECS;
		}

		//return engine activity
		public static string GetEngineActivity(string engineName)
		{
			//try to open connexion to the engine
			EngineClient client = EngineClientFromPool(engineName, $"GetEngineActivity");
			if (client == null) return null;

			string activityXML = client.GetServerActivity();
			//return client to pool
			EngineClientToPool(client);

			return activityXML;
		}

		//List index files - hdb folder content
		public static string GetEngineIndexDir(EngineClient client, string indexName)
        {
			if (client == null) return null; 

			string cmd = $"index-info:dir:{indexName}";
			client.GetServerInformation(cmd, out string res);

			return res;
		}

		public static bool GetUserRightsAsSqlStr(string userNameOrId, string domainName, out CCPrincipal principal, out string userRights)
		{
			userRights = null;

			principal = CC.Current.GetPrincipalAny(userNameOrId, domainName);
			if (principal == null)
			{
				Sys.LogError($"Cannot load user with Id or Name [{userNameOrId}] from domain [{domainName}]");
				return false;
			}

			long pid = CC.Current.NativeDomains.GetPrincipalByUserId(principal.UserId);
			if (pid == 0)
			{
				Sys.LogError($"Cannot get principal ID from native domains , principal ID [{principal.Id}] principal Name [{principal.Name}] from domain [{domainName}]");
				return false;
			}

			//use SearchSession in order to access userRights
			SearchSession SSession = new SearchSession();
			if (SSession.DoLogin(principal))
			{
				userRights = SSession.UserRightsAsSqlStr;
				Sys.Log2(50, $"User ID [{SSession.User.Id}] FullName [{SSession.User.FullName}] SQL rights [{userRights}]");
				SSession.DoLogout();
			}
			else
			{
				Sys.LogError($"Cannot authenticate user, principal ID [{principal.Id}] principal Name [{principal.Name}] from domain [{domainName}]");
                SSession.Dispose();
                return false;
			}
			SSession.Dispose();

			return true;
		}

		// Create recurive dir recursively 
		public static bool CreateDir(string folderPath)
		{
			//create output folder
			if (!Directory.Exists(folderPath))
			{
				try
				{
					Directory.CreateDirectory(folderPath);
				}
				catch (Exception e)
				{
					Sys.LogError($"Cannot create output directory [{folderPath}]");
					Sys.LogError(e);
					return false;
				}
			}
			return true;
		}

		public static bool DumpFile(string filePath, string content)
		{
			string folderPath = Str.PathGetDir(filePath);
			if (!Toolbox.CreateDir(folderPath)) return false;

			try
			{
				System.IO.File.WriteAllText(filePath, content, Encoding.UTF8);
			}
			catch (Exception e)
			{
				Sys.LogError($"Cannot write file [{filePath}] {e}");
				return false;
			}
			return true;
		}

		public static string GetOutputFilePath(string outputFolderPath, string fileName, string fileExt, string subFolder = null)
		{
			string folderPath = !String.IsNullOrEmpty(subFolder) ? Str.PathAdd(outputFolderPath, subFolder) : outputFolderPath;
			string fName = fileName + (fileExt.StartsWith(".") ? fileExt : "." + fileExt);
			if (!Str.IsValidFilename(fName))
			{
				Sys.LogError($"Invalid file name: [{fName}]");
				return null;
			}
			return Str.PathAdd(folderPath, fName);
		}

		public static bool DumpEnginesConfig(string outputFolderPath)
        {
            foreach (CCEngine ccEngine in CC.Current.Engines)
            {
				string path = Str.PathAdd(outputFolderPath, "Configuration", "Engines", $"{ccEngine.Name}.xml");
				if (!DumpFile(path, ccEngine.GetXDoc().OuterXml))
				{
					Sys.LogError($"Cannot dump Engine [{ccEngine.Name}] configuration to [{path}]");
					return false;
				}
			}
			Sys.Log($"Dump [{CC.Current.Engines.Count}] Engines configuration to [{Str.PathAdd(outputFolderPath, "Configuration", "Engines")}]");
			return true;
        }

		public static bool DumpIndexesConfig(string outputFolderPath)
        {
			foreach (CCIndex ccIndex in CC.Current.Indexes)
			{
				string path = Str.PathAdd(outputFolderPath, "Configuration", "Indexes", $"{ccIndex.Name}.xml");
				if (!DumpFile(path, ccIndex.GetXDoc().OuterXml))
				{
					Sys.LogError($"Cannot dump Index [{ccIndex.Name}] configuration to [{path}]");
					return false;
				}
			}
			Sys.Log($"Dump [{CC.Current.Indexes.Count}] Indexes configuration to [{Str.PathAdd(outputFolderPath, "Configuration", "Indexes")}]");
			return true;
		}

		public static bool DumpEnginesIndexDir(string outputFolderPath)
        {
			foreach (CCEngine ccEngine in CC.Current.Engines)
			{
				EngineClient client = EngineClientFromPool(ccEngine.Name, $"DumpEnginesIndexDir");
				if (client == null) return false;

				int dumpCount = 0;
				foreach (CCIndex ccIndex in ccEngine.Indexes)
                {
                    if (!client.ExistIndex(ccIndex.Name))
                    {
						Sys.LogWarning($"Cannot dump Index Dir [{ccEngine.Name}] [{ccIndex.Name}] index do not exist");
						continue;
                    }

					string idxDirContent = GetEngineIndexDir(client, ccIndex.Name);
					string path = Str.PathAdd(outputFolderPath, "IndexesDir", $"{ccEngine.Name}", $"{ccIndex.Name}.xml");
					if (!DumpFile(path, idxDirContent))
					{
						Sys.LogError($"Cannot dump Index Dir [{ccEngine.Name}] [{ccIndex.Name}] to [{path}]");
						return false;
					}
					dumpCount++;
				}
				Sys.Log($"Dump [{dumpCount}] Indexes directory information to [{Str.PathAdd(outputFolderPath, "IndexesDir", $"{ccEngine.Name}")}]");

				EngineClientToPool(client);
			}
			return true;
		}
	}

	public class EngineCustomStatus : IEquatable<EngineCustomStatus>
	{
		private bool _init = false;

		public CCEngine CCEngine { get; set; }
		public bool IsAlive { get; private set; } = false;
		public int ConnectionCount { get; private set; } = 0;
		public string Host { get; private set; } = Str.Empty;
		public string Name { get; private set; } = Str.Empty;
		public int Port { get; private set; } = 0;
		public DateTime StartTime { get; private set; } = new DateTime();
		public string Version { get; private set; } = Str.Empty;
		public long VMSize { get; private set; } = 0;
		public string SRPC { get; private set; }
		public ListOf<IndexStatus> IndexesStatus { get; private set; }
		public string ServerStatus { get; private set; }
		public XDocument XServerStatus { get; private set; }
		public Dictionary<string, (string indexName, long docs, long ghosts, long diskSizeBytes)> dIndexesStats { get; private set; } = new Dictionary<string, (string, long, long, long)>();

		public long TotalDocs
		{
			get { return dIndexesStats.Values.Sum(_ => _.docs); }
		}
		public long TotalGhosts
		{
			get { return dIndexesStats.Values.Sum(_ => _.ghosts); }
		}
		public long TotalDiskSizeBytes
		{
			get { return dIndexesStats.Values.Sum(_ => _.diskSizeBytes); }
		}


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
				this.SRPC = GetSRPC(client);
				this.IndexesStatus = client.GetIndexesStatus();
				this.ServerStatus = client.GetServerStatus();

				//TODO move away from constructor
				this.Init(client.Name);
			}
		}

		public bool Init(string clientName)
        {
			if (_init) return _init;

			this.XServerStatus = XDocument.Parse(ServerStatus);
			this.GetIndexesStats();
			this.CCEngine = CC.Current.Engines.GetAnyway(clientName);
			this._init = true;

			return _init;
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

		private string GetSRPC(EngineClient client)
		{
			return "srpc://" + client.Host + ":" + client.Port;
		}

		private void GetIndexesStats()
        {
			foreach (IndexStatus idxStatus in IndexesStatus)
			{
				long docs = idxStatus.DocumentCount;
				long ghosts = idxStatus.GhostCount;
				long diskSizeBytes = GetIndexSizeOnDiskBytes(idxStatus.Name);
				dIndexesStats.Add(idxStatus.Name, (idxStatus.Name, docs, ghosts, diskSizeBytes));
			}
		}

		private long GetIndexSizeOnDiskBytes(string indexName) => GetIndexSizeOnDiskKb(indexName) * 1024;

		private long GetIndexSizeOnDiskKb(string indexName)
		{
			long l = 0;

			if (XServerStatus != null)
			{
                try
                {
					XElement indexElem = XServerStatus.Root.Element("Indexes").Descendants("Index").SingleOrDefault(x => x.Attribute("Alias") != null && Str.EQNC(x.Attribute("Alias").Value, indexName));

					if (indexElem == null)
					{
						Sys.LogWarning($"ServerStatus - Cannot find index [{indexName}] [{XServerStatus}]");
						return -1;
					}
					l = long.Parse(indexElem.Element("IndexSizeKb").Value);
				}
                catch(Exception e)
                {
					Sys.LogError($"ServerStatus - index [{indexName}] {e}");
					Sys.LogError($"ServerStatus - index [{indexName}] from [{XServerStatus}]");
					return -1;
                }
			}
			return l;
		}

		

		public bool Equals(EngineCustomStatus other)
		{
			if (other is null)
				return false;

			return this.Name == other.Name && this.Host == other.Host && this.Port == other.Port;
		}

		public override bool Equals(object obj) => Equals(obj as EngineCustomStatus);
		public override int GetHashCode() => (Name, Host, Port).GetHashCode();

	}


	public static class EngineStatusHelper
	{
		private const string LABEL_HOST = "Host";
		private const string LABEL_DOC_COUNT = "Doc count";
		private const string LABEL_DOC_RATIO_TOTAL = "Doc Ratio";
		private const string LABEL_GHOST_COUNT = "Ghost Count";
		private const string LABEL_GHOST_RATIO_DOC = "Ghost/Doc Ratio";
		private const string LABEL_GHOST_RATIO_TOTAL = "Ghost Ratio";
		private const string LABEL_SIZE_DISK = "Size on Disk";
		private const string LABEL_SIZE_DISK_RATIO_TOTAL = "Size on Disk Ratio";
		private const string LABEL_IDX_COUNT = "Indexes Count";
		private const string LABEL_THREAD_COUNT = "Threads Count";

		public static List<EngineCustomStatus> GetEnginesStatus(List<CCEngine> lEngineConfig)
		{
			List<EngineCustomStatus> lEngineCustomStatus = new List<EngineCustomStatus>();
			foreach (CCEngine engine in lEngineConfig)
			{
				EngineCustomStatus ECS = Toolbox.GetEngineStatus(engine.FullName);
				if (ECS == null || !ECS.IsAlive)
				{
					Sys.LogError($"Can't connect to Engine [{engine.FullName}]");
					return null;
				}
				lEngineCustomStatus.Add(ECS);
			}
			return lEngineCustomStatus;
		}

		public static void LogIndexesStatus(EngineCustomStatus ECS)
		{
			Sys.Log($"Engine [{ECS.Name}] Host [{ECS.GetDisplayIPPort()}] Version [{ECS.Version}] VM Size [{ECS.GetDisplayVMSize()}] ");
			LogIndexesGrid(ECS);
		}

		private static void LogIndexesGrid(EngineCustomStatus ECS)
		{
			LogTable logTableIndexes = new LogTable(ECS.Name);
			logTableIndexes.SetInnerColumnSpaces(1, 1);

			foreach ((string indexName, long docs, long ghosts, long diskSizeBytes) in ECS.dIndexesStats.Values)
			{
				logTableIndexes.AddItem(indexName, LABEL_DOC_COUNT, docs.ToString("N0", CultureInfo.InvariantCulture));
				double docRatio = (double)docs / (double)ECS.TotalDocs;
				logTableIndexes.AddItem(indexName, LABEL_DOC_RATIO_TOTAL, docRatio.ToString("P", CultureInfo.InvariantCulture));
				logTableIndexes.AddItem(indexName, LABEL_GHOST_COUNT, ghosts.ToString("N0", CultureInfo.InvariantCulture));
				double ghostDocsRatio = docs == 0 ? 0 : (double)ghosts / (double)docs;
				logTableIndexes.AddItem(indexName, LABEL_GHOST_RATIO_DOC, ghostDocsRatio.ToString("P", CultureInfo.InvariantCulture));
				double ghostRatio = (double)ghosts / (double)ECS.TotalGhosts;
				logTableIndexes.AddItem(indexName, LABEL_GHOST_RATIO_TOTAL, ghostRatio.ToString("P", CultureInfo.InvariantCulture));
				logTableIndexes.AddItem(indexName, LABEL_SIZE_DISK, Str.Size(diskSizeBytes));
				double diskSizeBytesRatio = (double)diskSizeBytes / (double)ECS.TotalDiskSizeBytes;
				logTableIndexes.AddItem(indexName, LABEL_SIZE_DISK_RATIO_TOTAL, diskSizeBytesRatio.ToString("P", CultureInfo.InvariantCulture));
			}

			logTableIndexes.AddSeparatorBeforeRow("Total");
			logTableIndexes.AddItem("Total", LABEL_DOC_COUNT, ECS.TotalDocs.ToString("N0", CultureInfo.InvariantCulture));
			logTableIndexes.AddItem("Total", LABEL_DOC_RATIO_TOTAL, 1.ToString("P", CultureInfo.InvariantCulture));
			logTableIndexes.AddItem("Total", LABEL_GHOST_COUNT, ECS.TotalGhosts.ToString("N0", CultureInfo.InvariantCulture));
			logTableIndexes.AddItem("Total", LABEL_GHOST_RATIO_DOC, 1.ToString("P", CultureInfo.InvariantCulture));
			logTableIndexes.AddItem("Total", LABEL_GHOST_RATIO_TOTAL, 1.ToString("P", CultureInfo.InvariantCulture));
			logTableIndexes.AddItem("Total", LABEL_SIZE_DISK, Str.Size(ECS.TotalDiskSizeBytes));
			logTableIndexes.AddItem("Total", LABEL_SIZE_DISK_RATIO_TOTAL, 1.ToString("P", CultureInfo.InvariantCulture));

			logTableIndexes.SysLog();
		}

		public static void LogEnginesStatus(List<EngineCustomStatus> lEngineCustomStatus)
		{
			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Engine(s) Status");
			foreach (EngineCustomStatus ECS in lEngineCustomStatus) LogIndexesStatus(ECS);
			LogEnginesGrid(lEngineCustomStatus);
			Sys.Log($"----------------------------------------------------");
        }

		private static void LogEnginesGrid(List<EngineCustomStatus> lEngineCustomStatus)
		{
			LogTable logTableEngines = new LogTable("Engines");
			logTableEngines.SetInnerColumnSpaces(1, 1);

			long enginesTotalDocs = lEngineCustomStatus.Sum(_ => _.TotalDocs);
			long enginesTotalGhosts = lEngineCustomStatus.Sum(_ => _.TotalGhosts);
			long enginesTotalSizeOnDiskBytes = lEngineCustomStatus.Sum(_ => _.TotalDiskSizeBytes);
			long enginesTotalIndexes = lEngineCustomStatus.Sum(_ => _.IndexesStatus.Count);
			long enginesTotalThreads = lEngineCustomStatus.Sum(_ => _.CCEngine.ServerActiveThreads);

			foreach (EngineCustomStatus ECS in lEngineCustomStatus)
			{

				logTableEngines.AddItem(ECS.Name, LABEL_HOST, ECS.Host);
				logTableEngines.AddItem(ECS.Name, LABEL_DOC_COUNT, ECS.TotalDocs.ToString("N0", CultureInfo.InvariantCulture));
				double docRatio = (double)ECS.TotalDocs / (double)enginesTotalDocs;
				logTableEngines.AddItem(ECS.Name, LABEL_DOC_RATIO_TOTAL, docRatio.ToString("P", CultureInfo.InvariantCulture));
				logTableEngines.AddItem(ECS.Name, LABEL_GHOST_COUNT, ECS.TotalGhosts.ToString("N0", CultureInfo.InvariantCulture));
				double ghostDocRatio = ECS.TotalDocs == 0 ? 0 : (double)ECS.TotalGhosts / (double)ECS.TotalDocs;
				logTableEngines.AddItem(ECS.Name, LABEL_GHOST_RATIO_DOC, ghostDocRatio.ToString("P", CultureInfo.InvariantCulture));
				double ghostRatio = (double)ECS.TotalGhosts / (double)enginesTotalGhosts;
				logTableEngines.AddItem(ECS.Name, LABEL_GHOST_RATIO_TOTAL, ghostRatio.ToString("P", CultureInfo.InvariantCulture));
				logTableEngines.AddItem(ECS.Name, LABEL_SIZE_DISK, Str.Size(ECS.TotalDiskSizeBytes));
				double sizeOnDiskRatio = (double)ECS.TotalDiskSizeBytes / (double)enginesTotalSizeOnDiskBytes;
				logTableEngines.AddItem(ECS.Name, LABEL_SIZE_DISK_RATIO_TOTAL, sizeOnDiskRatio.ToString("P", CultureInfo.InvariantCulture));
				logTableEngines.AddItem(ECS.Name, LABEL_IDX_COUNT, ECS.IndexesStatus.Count.ToString("N0", CultureInfo.InvariantCulture));
				logTableEngines.AddItem(ECS.Name, LABEL_THREAD_COUNT, ECS.CCEngine.ServerActiveThreads.ToString("N0", CultureInfo.InvariantCulture));
			}

			logTableEngines.AddSeparatorBeforeRow("Total");
			logTableEngines.AddItem("Total", LABEL_HOST, "");
			logTableEngines.AddItem("Total", LABEL_DOC_COUNT, enginesTotalDocs.ToString("N0", CultureInfo.InvariantCulture));
			logTableEngines.AddItem("Total", LABEL_DOC_RATIO_TOTAL, 1.ToString("P", CultureInfo.InvariantCulture));
			logTableEngines.AddItem("Total", LABEL_GHOST_COUNT, enginesTotalGhosts.ToString("N0", CultureInfo.InvariantCulture));
			logTableEngines.AddItem("Total", LABEL_GHOST_RATIO_DOC, 1.ToString("P", CultureInfo.InvariantCulture));
			logTableEngines.AddItem("Total", LABEL_GHOST_RATIO_TOTAL, 1.ToString("P", CultureInfo.InvariantCulture));
			logTableEngines.AddItem("Total", LABEL_SIZE_DISK, Str.Size(enginesTotalSizeOnDiskBytes));
			logTableEngines.AddItem("Total", LABEL_SIZE_DISK_RATIO_TOTAL, 1.ToString("P", CultureInfo.InvariantCulture));
			logTableEngines.AddItem("Total", LABEL_IDX_COUNT, enginesTotalIndexes.ToString("N0", CultureInfo.InvariantCulture));
			logTableEngines.AddItem("Total", LABEL_THREAD_COUNT, enginesTotalThreads.ToString("N0", CultureInfo.InvariantCulture));

			logTableEngines.SysLog();
		}

		public static long LogIndexesChanges(List<EngineCustomStatus> A, List<EngineCustomStatus> B)
		{
			long totalChanges = 0;

			if (A.Except(B).Count() != 0)
			{
				Sys.LogError($"Cannot compare different lists of Engine Status");
				return -1;
			}

			foreach (EngineCustomStatus A_ECS in A)
			{
				LogTable logTabeIndexesChanges = new LogTable($"Engine [{A_ECS.Name}]");
				logTabeIndexesChanges.SetInnerColumnSpaces(1, 1);

				foreach (IndexStatus A_ECS_IDX in A_ECS.IndexesStatus)
				{
					IndexStatus B_ECS_IDX = B.Single(B_ECS => Str.EQ(B_ECS.Name, A_ECS.Name)).IndexesStatus.Single(x => Str.EQ(x.Name, A_ECS_IDX.Name));

					long delta = B_ECS_IDX.DocumentCount - A_ECS_IDX.DocumentCount;
					logTabeIndexesChanges.AddItem(A_ECS_IDX.Name, "From", A_ECS_IDX.DocumentCount.ToString("+0.#;-0.#;0"));
					logTabeIndexesChanges.AddItem(A_ECS_IDX.Name, "To", B_ECS_IDX.DocumentCount.ToString("+0.#;-0.#;0"));
					logTabeIndexesChanges.AddItem(A_ECS_IDX.Name, "Change", delta.ToString("+0.#;-0.#;0"));
					totalChanges += Math.Abs(delta);
				}

				logTabeIndexesChanges.SysLog();
			}

			return totalChanges;
		}

	}

	public class LogTable
	{
		private char _rowSep = '-';
		private char _colSep = '|';

		private int _innerLeftColSPaces = 0;
		private int _innerRightColSPaces = 0;

		private bool _logHeader = true;

		private List<LogTableItem> _lItems = new List<LogTableItem>();
		private List<string> _lSepearatorsBefore = new List<string>();
		private List<string> _lSepearatorsAfter = new List<string>();

		private List<string> _lColumns = new List<string>();
		private List<string> _lRows = new List<string>();

		private Dictionary<string, int> _dColumnsMaxLength = new Dictionary<string, int>();

		private string _name;

		private int _firstColumnMaxLength = 0;

		public LogTable(string tableName = Str.Empty)
		{
			if (tableName == null) throw new ArgumentException("cannot be set to null", "tableName");

			this._name = tableName;
			SetFistColumnMaxLength(_name);
		}

		public void SetRowSeparatorChar(char rowSep) { this._rowSep = rowSep; }

		public void SetColumnSeparatorChar(char colSep) { this._colSep = colSep; }

		public void SetInnerColumnSpaces(int left, int right)
		{
			if (left > 0) this._innerLeftColSPaces = left;
			if (right > 0) this._innerRightColSPaces = right;
		}

		public void SetLogHeader(bool logHeader) { this._logHeader = logHeader; }

		public bool AddItem(string rowName, string columnName, string value)
		{
			if (String.IsNullOrEmpty(rowName) || String.IsNullOrEmpty(columnName)) return false;

			_lItems.Add(new LogTableItem(rowName, columnName, value));

			_lRows.AddUnique(rowName);
			SetFistColumnMaxLength(rowName);

			_lColumns.AddUnique(columnName);
			SetColumnMaxLength(columnName, value);

			return true;
		}

		public bool AddItems(List<(string rowName, string columnName, string value)> lItems)
		{
			bool bOk = true;
			foreach ((string rowName, string columnName, string value) item in lItems)
			{
				bOk = AddItem(item.rowName, item.columnName, item.value);
				if (!bOk) return bOk;
			}
			return bOk;
		}

		public bool AddUniqueItem(string rowName, string columnName, string value)
		{
			if (_lItems.SingleOrDefault(x => Str.EQ(x.row, rowName) && Str.EQ(x.column, columnName) && Str.EQ(x.value, value)) == null) return AddItem(rowName, columnName, value);
			return true;
		}

		public void AddSeparatorBeforeRow(string rowName)
		{
			_lSepearatorsBefore.AddUnique(rowName);
		}

		public void AddSeparatorAfterRow(string rowName)
		{
			_lSepearatorsAfter.AddUnique(rowName);
		}

		private void SetFistColumnMaxLength(string token)
		{
			if (token.Length > _firstColumnMaxLength) _firstColumnMaxLength = token.Length;
		}

		private void SetColumnMaxLength(string column, string value)
		{
			if (_dColumnsMaxLength.ContainsKey(column))
			{
				if (value.Length > _dColumnsMaxLength[column]) _dColumnsMaxLength[column] = value.Length;
			}
			else
			{
				int max = column.Length > value.Length ? column.Length : value.Length;
				_dColumnsMaxLength.Add(column, max);
			}
		}

		public void SysLog() { Sys.Log(Log()); }

		public void SysLog2(int logLevel) { Sys.Log2(logLevel, Log()); }

		public void ConsoleLog() { Console.WriteLine(Log()); }

		private string Log()
		{
			StringBuilder sb = new StringBuilder();
			sb.AppendLine();
			sb.AppendLine(GetLineSep());
			if (_logHeader)
			{
				sb.AppendLine(GetHeaders());
				sb.AppendLine(GetLineSep());
			}
			foreach (string row in _lRows)
			{
				if (_lSepearatorsBefore.Contains(row)) sb.AppendLine(GetLineSep());
				sb.AppendLine(GetRow(row));
				if (_lSepearatorsAfter.Contains(row)) sb.AppendLine(GetLineSep());
			}
			sb.AppendLine(GetLineSep());
			return sb.ToString();
		}

		private string GetSepInnerLeft() { return Str.Empty.PadLeft(_innerLeftColSPaces, _rowSep); }

		private string GetSepInnerRight() { return Str.Empty.PadLeft(_innerRightColSPaces, _rowSep); }

		private string GetSepToken(PadDirection direction, int colLength)
		{
			string s = null;

			if (direction == PadDirection.Left)
			{
				s = _rowSep + GetSepInnerLeft() + Str.Empty.PadLeft(colLength, _rowSep) + GetSepInnerRight();
			}
			else if (direction == PadDirection.Right)
			{
				s = _rowSep + GetSepInnerLeft() + Str.Empty.PadRight(colLength, _rowSep) + GetSepInnerRight();
			}
			return s;
		}

		private string GetCellInnerLeft() { return Str.Empty.PadLeft(_innerLeftColSPaces); }

		private string GetCellInnerRight() { return Str.Empty.PadLeft(_innerRightColSPaces); }

		private string GetCellToken(string value, PadDirection direction, int colLength)
		{
			string s = null;
			if (direction == PadDirection.Left)
			{
				s = _colSep + GetCellInnerLeft() + value.PadLeft(colLength) + GetCellInnerRight();
			}
			else if (direction == PadDirection.Right)
			{
				s = _colSep + GetCellInnerLeft() + value.PadRight(colLength) + GetCellInnerRight();
			}
			return s;
		}

		private string GetLineSep()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(GetSepToken(PadDirection.Left, _firstColumnMaxLength));
			//foreach (int columnMaxLength in _dColumnsMaxLength.Values) sb.Append(GetSepToken(PadDirection.Left, columnMaxLength));
			foreach (string column in _lColumns) sb.Append(GetSepToken(PadDirection.Left, _dColumnsMaxLength[column]));
			sb.Append(_rowSep);
			return sb.ToString();
		}

		private string GetHeaders()
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(GetCellToken(_name, PadDirection.Right, _firstColumnMaxLength));
			foreach (string column in _lColumns) sb.Append(GetCellToken(column, PadDirection.Left, _dColumnsMaxLength[column]));
			sb.Append(_colSep);
			return sb.ToString();
		}

		private string GetRow(string rowName)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(GetCellToken(rowName, PadDirection.Right, _firstColumnMaxLength));
			foreach (string column in _lColumns)
			{
				LogTableItem itm = _lItems.SingleOrDefault(item => Str.EQNC(item.row, rowName) && Str.EQNC(item.column, column));
				string value = itm != null ? itm.value : Str.Empty;
				sb.Append(GetCellToken(value, PadDirection.Left, _dColumnsMaxLength[column]));
			}
			sb.Append(_colSep);
			return sb.ToString();
		}

		internal enum PadDirection
		{
			Left,
			Right
		}

		internal class LogTableItem
		{
			public string row { get; private set; }
			public string column { get; private set; }
			public string value { get; private set; }

			public LogTableItem(string row, string column, string value)
			{
				this.row = row;
				this.column = column;
				this.value = value;
			}
		}

	}

}
