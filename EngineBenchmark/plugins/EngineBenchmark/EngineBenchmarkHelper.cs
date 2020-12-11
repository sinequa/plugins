using System;
using System.Text;
using Sinequa.Common;
using Sinequa.Configuration;
using Sinequa.Search;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;

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

	public static class Toolbox
	{
		//Get engine clients
		//please make sure client is return to the pool before the end of execution.
		public static EngineClient EngineClientFromPool(string engineName)
		{
			EngineClient client = null;
			try
			{
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
			EngineClient client = EngineClientFromPool(engineName);
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
			EngineClient client = EngineClientFromPool(engineName);
			if (client == null) return null;

			string activityXML = client.GetServerActivity();
			//return client to pool
			EngineClientToPool(client);

			return activityXML;
		}

		public static bool GetUserRightsAsSqlStr(string userNameOrId, string domainName, SecuritySyntax securitySyntax, out CCPrincipal principal, out string userRights)
		{
			userRights = null;

			principal = CC.Current.GetPrincipalAny(userNameOrId, domainName);
			if (principal == null)
			{
				Sys.LogError($"Cannot load user with Id or Name [{userNameOrId}] in domain [{domainName}]");
				return false;
			}

			long pid = CC.Current.NativeDomains.GetPrincipalByUserId(principal.UserId);
			if (pid == 0)
			{
				Sys.LogError($"Cannot get principal ID from native domains , user name [{principal.Name}]");
				return false;
			}

			if (securitySyntax == SecuritySyntax.Engine)
			{
				userRights = RightsAsSqlStrXb(principal.UserId, new ListStr(), new ListStr());
			}
			else if (securitySyntax == SecuritySyntax.Legacy)
			{
				int flags = NativeDomains.UR_USE_CACHE | NativeDomains.UR_DO_USER_LSTSTR | NativeDomains.UR_DO_USER_SQLSTR;

				if (!CC.Current.NativeDomains.CalculateUserRightsWithCache(pid, null, null, flags, out ListStr user_list,
					out ListStr field_list, out string user_rights_sql, out string xfield_parts, out string fingerprint))
				{
					Sys.LogError($"Cannot calculate user rights from native domains for user name [{principal.Name}]");
					return false;
				}

				userRights = RightsAsSqlStr(user_rights_sql);
			}

			if (userRights == null)
			{
				Sys.LogError($"Cannot build SQL rights for user name [{principal.Name}]");
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
		public string SRPC { get; private set; }
		public ListOf<IndexStatus> indexesStatus { get; private set; }

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
				this.indexesStatus = client.GetIndexesStatus();
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

		private string GetSRPC(EngineClient client)
		{
			return "srpc://" + client.Host + ":" + client.Port;
		}

		public void LogIndexesGrid()
        {
			List<(string row, string column, string value)> lGridRows = new List<(string row, string column, string value)>();
			long totalDocs = 0;
			long totalGhosts = 0;
			foreach (IndexStatus idxStatus in indexesStatus)
			{
				long docs = idxStatus.DocumentCount;
				lGridRows.Add(($"{idxStatus.Name}", "Document Count", docs.ToString("N0", CultureInfo.InvariantCulture)));
				totalDocs += docs;

				long ghosts = idxStatus.DocumentCount;
				lGridRows.Add(($"{idxStatus.Name}", "Ghost Count", ghosts.ToString("N0", CultureInfo.InvariantCulture)));
				totalGhosts += ghosts;
			}
			lGridRows.Add(("Total", "Document Count", totalDocs.ToString("N0", CultureInfo.InvariantCulture)));
			lGridRows.Add(("Total", "Ghost Count", totalGhosts.ToString("N0", CultureInfo.InvariantCulture)));
			LogArray LA = new LogArray(lGridRows, $"{Name}");
			LA.Log();
		}

		public void Log(bool indexesStatus = false)
		{
			Sys.Log($"Engine [{this.Name}] Host [{this.GetDisplayIPPort()}] Version [{this.Version}] VM Size [{this.GetDisplayVMSize()}] ");
			if (indexesStatus) LogIndexesGrid();
		}

	}

	public class LogArray
	{
		private readonly char LINESEP = '-';
		private readonly char COLSEP = '|';
		private readonly string EMPTYSEP = " ";

		private List<(string row, string column, string value)> _l;
		private List<string> _lColumns;
		private List<string> _lRows;
		private Dictionary<string, int> _dColumnsMaxLength = new Dictionary<string, int>();
		private string _name;

		private bool _init = false;
		private int _firstColumnMaxLength = -1;

		public LogArray(List<(string row, string column, string value)> l, string name = Str.Empty)
		{
			this._l = l;
			this._name = name;
		}

		public void Log()
		{
			if (!_init) Init();

			Sys.Log(LineSep());
			Sys.Log(Header());
			Sys.Log(LineSep());
			foreach (string r in _lRows)
			{
				Sys.Log(Row(r));
			}
			Sys.Log(LineSep());
		}

		public bool Init()
		{
			if (_init) return true;

			if (_l == null || _l.Count == 0) return false;

			_lColumns = _l.Select(x => x.column).Distinct().ToList();
			if (_lColumns == null || _lColumns.Count == 0) return false;

			_lRows = _l.Select(x => x.row).Distinct().ToList();
			if (_lRows == null || _lRows.Count == 0) return false;

			int nameLength = _name.Length;
			int maxRowNameLength = _l.Max(x => x.row.Length);
			_firstColumnMaxLength = nameLength >= maxRowNameLength ? nameLength : maxRowNameLength;

			GetColumnsMaxLength();

			_init = true;

			return true;
		}

		private void GetColumnsMaxLength()
		{
			foreach (string column in _lColumns)
			{
				_dColumnsMaxLength.Add(column, GetColumnMaxLength(column));
			}
		}

		private int GetColumnMaxLength(string columnName)
		{
			int columnNameLength = columnName.Length;
			int columnValueMaxLength = _l.Where(x => Str.EQNC(x.column, columnName)).Max(x => x.value.Length);
			return columnNameLength >= columnValueMaxLength ? columnNameLength : columnValueMaxLength;
		}

		private string Row(string rowName)
		{
			StringBuilder sb = new StringBuilder();
			//first column
			sb.Append(COLSEP + rowName.PadRight(_firstColumnMaxLength) + COLSEP);
			//columns
			foreach (string column in _lColumns)
			{
				string value = _l.Single(x => Str.EQNC(x.row, rowName) && Str.EQNC(x.column, column)).value;
				sb.Append(value.PadLeft(_dColumnsMaxLength[column]) + COLSEP);
			}
			return sb.ToString();
		}

		private string Header()
		{
			StringBuilder sb = new StringBuilder();
			//first column
			sb.Append(COLSEP + _name.PadRight(_firstColumnMaxLength) + COLSEP);
			//columns
			foreach (string column in _lColumns)
			{
				sb.Append(column.PadLeft(_dColumnsMaxLength[column]) + COLSEP);
			}
			return sb.ToString();
		}

		private string LineSep()
		{
			StringBuilder sb = new StringBuilder();
			//first column
			sb.Append(LINESEP + Str.Empty.PadLeft(_firstColumnMaxLength, LINESEP) + LINESEP);
			//columns
			foreach (int columnMaxLength in _dColumnsMaxLength.Values) sb.Append(Str.Empty.PadLeft(columnMaxLength, LINESEP) + LINESEP);
			return sb.ToString();
		}
	}

}
