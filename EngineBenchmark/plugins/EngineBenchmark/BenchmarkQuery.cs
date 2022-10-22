using System;
using System.Collections.Generic;
using Sinequa.Common;
using Sinequa.Search;
using System.Linq;
using System.Diagnostics;
using System.Text;
using System.Globalization;

namespace Sinequa.Plugin
{
	public class BenchmarkQuery : IDisposable
	{
		public int threadId { get; private set; }
		public string threadGroupName { get; private set; }
		public int iteration { get; private set; }
		public string engineName { get; private set; }
		public string sql { get; private set; }
		public bool success { get; private set; }
		
		//timers
		public long clientFromPoolTimer { get; private set; }
		public long clientToPoolTimer { get; private set; }
		public long totalQueryTimer { get; private set; }
		public long execCursorTimer { get; private set; }
		public long readCursorTimer { get; private set; }
		public double cursorNetworkAndDeserializationTimer { get; private set; }

		//main cursor attributes
		public int cacheHit { get; private set; }
		public double rowFetchTime { get; private set; }
		public double processingTime { get; private set; }
		public long matchingRowCount { get; private set; }
		public long postGroupByMatchingRowCount { get; private set; }

		//cursor values
		public int cursortotalRowCount { get; private set; }
		public int cursorRowCount { get; private set; }
		public int cursorAttributeCount { get; private set; }
		public int cursorColumnCount { get; private set; }

		//optional cursor attributes
		public string internalQueryLog { get; private set; }
		public Dictionary<string, string> dInternalQueryAnalysis { get; private set; } = new Dictionary<string, string>();

		//cursor size breakdown - represent the cursor attributes / columns sizes
		public Dictionary<string, long> cursorSizeBreakdown { get; private set; }
		public long cursorSize
		{
			get { return cursorSizeBreakdown != null ? cursorSizeBreakdown.Sum(x => x.Value) : 0; }
		}

		public double cursorSizeMB
        {
			get { return cursorSize / 1000000; }
		}

		private EngineClient _client;
		private Engine.Client.Cursor _cursor;
		private ThreadGroup _threadGroup;

		public BenchmarkQuery(int threadId, string threadGroupName, int iteration, string engineName, string sql, ThreadGroup threadGroup)
		{
			this.threadId = threadId;
			this.threadGroupName = threadGroupName;
			this.iteration = iteration;
			this.engineName = engineName;
			this.sql = sql;
			this._threadGroup = threadGroup;

			this.success = false;
			this.clientFromPoolTimer = 0;
			this.clientToPoolTimer = 0;
			this.totalQueryTimer = 0;
			this.execCursorTimer = 0;
			this.readCursorTimer = 0;
			this.cursorNetworkAndDeserializationTimer = 0;

			this.cursortotalRowCount = 0;
			this.cursorRowCount = 0;
			this.cursorAttributeCount = 0;
			this.cursorColumnCount = 0;

			this.cursorSizeBreakdown = new Dictionary<string, long>();
		}

		public bool Execute(bool simulate)
		{
			if (simulate)
			{
				Sys.LogWarning($@"{{{threadId}}} /!\ SIMULATE MODE - query will NOT be executed /!\");
				return true;
			}

			//get engine client session
			EngineClientFromPool();

			if (_client == null)
			{
				this.success = false;
				return false;
			}

			try
			{
				if (!ExecCursor()) return false;

				if ((_cursor != null))
				{
					//TotalRowCount
					cursortotalRowCount = _cursor.TotalRowCount;
					Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] cursor total Row Count [{cursortotalRowCount}]");
					cursorRowCount = _cursor.CursorRowCount;
					Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] cursor Row Count [{cursorRowCount}]");
					cursorAttributeCount = _cursor.AttributeCount;
					Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] cursor Attribute Count [{cursorAttributeCount}]");
					cursorColumnCount = _cursor.ColumnCount;
					Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] cursor Column Count [{cursorColumnCount}]");

					//attributes - cachehit - matchingrowcount - postgroupbymatchingrowcount - processingtime - rowfetchtime
					if (!GetCursorMainAttributes())
					{
						this.success = false;
						Sys.LogError($"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] cannot parse cursor attributes");
						return false;
					}

					//cursor Network And Deserialization Timer = execCursorTimer - processingTime
					cursorNetworkAndDeserializationTimer = execCursorTimer - processingTime;
					if (cursorNetworkAndDeserializationTimer < 0) cursorNetworkAndDeserializationTimer = 0;

					//optional attributes - Internal Query Log - Internal Query Analysis
					GetCursorIQLIQAAttribute();

					//read cursor
					if (!ReadCursor())
					{
						this.success = false;
						Sys.LogError($"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] cannot read cursor");
						return false;
					}
				}
			}
			catch (Exception ex)
			{
				this.success = false;

				Sys.LogError($"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] execCursor: {ex}");
				Sys.LogError($"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] error for SQL statement: {sql}");

				return false;
			}
			finally
			{
				try
				{
					//close cursor
					if (_cursor != null) _cursor.Close();
				}
				catch (Exception ex)
				{
					Sys.LogError($"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] close cursor error: {ex}");
				}
				//engine client session to pool
				EngineClientToPool();
			}

			this.success = true;
			return true;
		}

		private void EngineClientFromPool()
		{
			Stopwatch sw = new Stopwatch();
			sw.Start();
			_client = Toolbox.EngineClientFromPool(engineName, $"Thread group [{threadGroupName}][{iteration}]");
			sw.Stop();
			clientFromPoolTimer = sw.ElapsedMilliseconds;
			AddToTotalTime(clientFromPoolTimer);
			sw.Reset();

			Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] EngineClientFromPool [{Sys.TimerGetText(clientFromPoolTimer)}]");
		}

		private void EngineClientToPool()
		{
			if (_client == null) return;
			Stopwatch sw = new Stopwatch();
			sw.Start();
			Toolbox.EngineClientToPool(_client);
			sw.Stop();
			clientToPoolTimer = sw.ElapsedMilliseconds;
			AddToTotalTime(clientToPoolTimer);
			sw.Reset();

			Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] EngineClientToPool [{Sys.TimerGetText(clientToPoolTimer)}]");
		}

		private void AddToTotalTime(long timer)
		{
			totalQueryTimer += timer;
		}

		private bool ExecCursor()
		{
			if (_client == null) return false;

			Stopwatch swClientCursorExecute = new Stopwatch();
			swClientCursorExecute.Start();
			_cursor = _client.ExecCursor(sql);
			swClientCursorExecute.Stop();
			execCursorTimer = swClientCursorExecute.ElapsedMilliseconds;
			AddToTotalTime(execCursorTimer);
			swClientCursorExecute.Reset();

			Sys.Log2(100, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] SQL  [{sql}]");

			Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] ExecCursor [{Sys.TimerGetText(execCursorTimer)}]");

			if (_cursor == null) return false;
			return true;
		}

		private bool GetCursorMainAttributes()
		{
			if (_cursor == null) return false;

            if (!double.TryParse(Str.ParseToSep(_cursor.GetAttribute("processingtime"), ' '), out double attrProcessingtime)) return false;
			this.processingTime = attrProcessingtime;
			if (!double.TryParse(Str.ParseToSep(_cursor.GetAttribute("rowfetchtime"), ' '), out double attrRowfetchtime)) return false;
			this.rowFetchTime = attrRowfetchtime;
			if (!int.TryParse(_cursor.GetAttribute("cachehit"), out int attrCacheHit)) return false;
			this.cacheHit = attrCacheHit;
			if (!long.TryParse(_cursor.GetAttribute("matchingrowcount"), out long attrMatchingRowCount)) return false;
			this.matchingRowCount = attrMatchingRowCount;
			if (!long.TryParse(_cursor.GetAttribute("postgroupbymatchingrowcount"), out long attrPostGroupByMatchingRowCount)) return false;
			this.postGroupByMatchingRowCount = attrPostGroupByMatchingRowCount;
			return true;
		}

		private void GetCursorIQLIQAAttribute()
		{
			if (_cursor.HasAttribute("internalquerylog")) internalQueryLog = _cursor.GetAttribute("internalquerylog");
			//if (_cursor.HasAttribute("internalqueryanalysis")) internalQueryAnalysis = _cursor.GetAttribute("internalqueryanalysis");
			foreach (string attributeName in _cursor.AttributesNames)
				if (Str.BeginWith(attributeName, "internalqueryanalysis", false)) 
					if(!Str.IsEmpty(_cursor.GetAttribute(attributeName))) dInternalQueryAnalysis.Add(attributeName, _cursor.GetAttribute(attributeName));
		}

		private bool ReadCursor()
		{
			if (_cursor == null) return false;
			StringBuilder sbDumpCursor = new StringBuilder();

			Stopwatch swReadCursor = new Stopwatch();
			swReadCursor.Start();
			
			_cursor.Begin();

			string attributeName, attributeValue, columnName, columnValue = Str.Empty;
			int attributeSize, columnSize = 0;
			//read all attributes (once)
			foreach (string attrName in _cursor.AttributesNames)
			{
				attributeName = $"[attribute] {attrName}";
				attributeValue = _cursor.GetAttribute(attrName);
				swReadCursor.Stop();    //stop watch while add/update dictionnary and dump

				//attribute size
				attributeSize = attributeValue.Length;				
				if (cursorSizeBreakdown.ContainsKey(attributeName)) cursorSizeBreakdown[attributeName] += attributeSize;
				else cursorSizeBreakdown.Add(attributeName, attributeSize);
				
				//dump attribute name/value
				if (_threadGroup.conf.dumpCursor)
				{
					sbDumpCursor.AppendLine($"{attributeName}{_threadGroup.conf.outputCSVSeparator}{attributeValue}");
				}

				//restart watch
				swReadCursor.Start();
			}

			//dump column name
			if (_threadGroup.conf.dumpCursor)
			{
				for (int i = 0; i < _cursor.ColumnCount; i++)
				{
					columnName = $"[column] {_cursor.GetColumnName(i)}";
					sbDumpCursor.Append($"{columnName}{_threadGroup.conf.outputCSVSeparator}");
				}
				sbDumpCursor.Append(Environment.NewLine);
			}

			while (!_cursor.End() && _cursor.CursorRowCount > 0)
			{
				//read all columns from the cursor (all rows)
				for (int i = 0; i < _cursor.ColumnCount; i++)
				{
					columnName = $"[column] {_cursor.GetColumnName(i)}";
					columnValue = _cursor.GetColumn(i);
					swReadCursor.Stop();  //stop watch while add/update dictionnary and dump

					//column value size
					columnSize = columnValue.Length;
					if (cursorSizeBreakdown.ContainsKey(columnName)) cursorSizeBreakdown[columnName] += columnSize;
					else cursorSizeBreakdown.Add(columnName, columnSize);
					
					//dump column value
					if (_threadGroup.conf.dumpCursor) sbDumpCursor.Append($"{columnValue}{_threadGroup.conf.outputCSVSeparator}");	
				}
				if (_threadGroup.conf.dumpCursor) sbDumpCursor.Append(Environment.NewLine);

				//restart watch
				swReadCursor.Start();
				_cursor.MoveNext();
			}

			swReadCursor.Stop();
			readCursorTimer = swReadCursor.ElapsedMilliseconds;
			AddToTotalTime(readCursorTimer);
			swReadCursor.Reset();

			Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] ReadCursor [{Sys.TimerGetText(readCursorTimer)}]");

			//write cursor
			//TODO - dump separator exist in column / attr ?
			//TODO use log array ?
			if (_threadGroup.conf.dumpCursor && processingTime >= _threadGroup.conf.dumpCursorMinProcessingTime && cursorSizeMB >= _threadGroup.conf.dumpCursorMinSize)
            {
				string dumpQueryFilePath = Toolbox.GetOutputFilePath(_threadGroup.conf.outputFolderPath, $"cursor_{_threadGroup.name}_{iteration}", "csv", "Cursors");

				Stopwatch swDumpCursor = new Stopwatch();
				swDumpCursor.Start();
				if (Toolbox.DumpFile(dumpQueryFilePath, sbDumpCursor.ToString()))
				{
					swDumpCursor.Stop();
					Sys.Log($"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] Create Cursor dump [{dumpQueryFilePath}] [{Sys.TimerGetText(swDumpCursor.ElapsedMilliseconds)}]");
				}
				
            }
            else
            {
				Sys.Log2(50, $"{{{threadId}}} Thread group [{threadGroupName}][{iteration}] Skip Cursor dump, ProcessingTime [{processingTime.ToString()}] < Minimum query processing time [{_threadGroup.conf.dumpCursorMinProcessingTime.ToString()}] or Cursor Size [{cursorSizeMB.ToString()}] < Minimun cursor size [{_threadGroup.conf.dumpCursorMinSize.ToString()}]");
			}

			return true;
		}

		protected virtual void Dispose(bool disposing) { }

		public void Dispose()
		{
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
