using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Sinequa.Common;
using Sinequa.Configuration;

namespace Sinequa.Plugin
{

	public class EngineActivityManager
	{

		private ConcurrentDictionary<string, List<EngineActivityItem>> _dItemStore;
		private Dictionary<CCEngine, CancellationTokenSource> _dCancellationTokenSource = new Dictionary<CCEngine, CancellationTokenSource>();
		private ListOf<CCEngine> _lEngines;
		private int _frequency;
		private bool _dump;
		private EngineBenchmark _engineBenchmark;
		public bool isRunning { get; private set; } = false;

		private string _statsFormatCPU = "N0";
		private string _statsFormatCPUChange = "+0.#;-0.#;0";	//+N1;-N1;0
		private string _statsFormatGb = "N2";
		private string _statsFormatGbChange = "+0.##;-0.##;0";  //+N2;-N2;0
		private string _statsFormatCount = "N0";
		private string _statsFormatThreads = "N1";
		private string _statsFormatThreadsChange = "+0.##;-0.##;0"; //+N2;-N2;0

		public List<string> lStoreEngine
		{
			get
			{
				return _dItemStore.Keys.ToList();
			}
		}

		public List<EngineActivityItem> lEngineActivityItem
		{
			get
			{
				return _dItemStore.Values.Aggregate(new List<EngineActivityItem>(), (x, y) => x.Concat(y).ToList());
			}
		}

		public EngineActivityManager(ListOf<CCEngine> lEngines, int activityFrequency, bool dump, EngineBenchmark engineBenchmarkCommand)
		{
			this._dItemStore = new ConcurrentDictionary<string, List<EngineActivityItem>>();
			this._lEngines = lEngines;
			this._frequency = activityFrequency;
			this._engineBenchmark = engineBenchmarkCommand;
			this._dump = dump;
		}

		public void StartPeriodicEngineActivity()
		{
			if (isRunning) return;

			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Engine Activity Monitoring");
			foreach (CCEngine engine in _lEngines)
			{
				CancellationTokenSource source = new CancellationTokenSource();
				_dCancellationTokenSource.Add(engine, source);
				Sys.Log($"Start periodic Engine activity monitoring on Engine [{engine.Name}], frequency [{_frequency} ms]");
				PeriodicEngineActivity(engine.Name, _frequency, source.Token);
			}
			Sys.Log($"----------------------------------------------------");

			isRunning = true;
		}

		public void StopPeriodicEngineActivity()
		{
			if (!isRunning) return;

			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Engine Activity Monitoring");
			foreach (CCEngine engine in _dCancellationTokenSource.Keys)
			{
				_dCancellationTokenSource.TryGetValue(engine, out CancellationTokenSource source);
				source.Cancel();
				source.Dispose();
				Sys.Log($"Stop periodic Engine activity monitoring on Engine [{engine.Name}]");
			}
			Sys.Log($"----------------------------------------------------");

			isRunning = false;
		}

		private async Task PeriodicEngineActivity(string engineName, int millisecondsDelay, CancellationToken cancellationToken)
		{
			while (true)
			{
				string activityXML = Toolbox.GetEngineActivity(engineName);
				if (activityXML != null)
				{
					StoreActivity(engineName, activityXML);
				}
				await Task.Delay(millisecondsDelay, cancellationToken);
			}
		}

		public bool StoreActivity(string engineName, string activityXML)
		{
			EngineActivityItem item = new EngineActivityItem(engineName);
			if (!item.Parse(activityXML)) return false;

			List<EngineActivityItem> l = _dItemStore.AddOrUpdate(engineName, new List<EngineActivityItem> { item }, (k, v) => { v.Add(item); return v; });

			item.LogItem();
			if (_dump) Dump(engineName, activityXML, l.Count);

			return true;
		}

		private bool Dump(string engineName, string activityXML, int iteration)
		{
			Stopwatch swDump = new Stopwatch();
			swDump.Start();

			string dumpEngineActivityFilePath = Toolbox.GetOutputFilePath(_engineBenchmark.conf.outputFolderPath,  $"activity_{engineName}_{iteration}", "xml", "EngineActivity");

			if (Toolbox.DumpFile(dumpEngineActivityFilePath, activityXML))
			{
				swDump.Stop();
				Sys.Log($"Create EngineActivity XML dump [{dumpEngineActivityFilePath}] [{Sys.TimerGetText(swDump.ElapsedMilliseconds)}]");
				return true;
			}
			return false;
		}

		private List<EngineActivityItem> GetEngineActivityList(string engineName)
		{
			if (!_dItemStore.TryGetValue(engineName, out List<EngineActivityItem> lItems))
			{
				return null;
			}
			return lItems;
		}

		public void LogStats()
		{
			Stopwatch swStats = new Stopwatch();
			swStats.Start();

			Sys.Log($"----------------------------------------------------");
			Sys.Log($"Engine Activity Statistics");
			Sys.Log($"----------------------------------------------------");

			List<(string row, string column, string value)> lEngineActivityStats = new List<(string row, string column, string value)>();


			foreach (string engineName in lStoreEngine)
			{
				//engine X does not exist
				List<EngineActivityItem> lItems = GetEngineActivityList(engineName);
				if (lItems == null || lItems.Count == 0)
				{
					Sys.LogError($"Cannot get engine activity item for Engine [{engineName}]");
					continue;
				}

				EngineActivityItem first = lItems.OrderBy(x => x.processNow).First();
				EngineActivityItem last = lItems.OrderBy(x => x.processNow).Last();

				//activity monitored count
				lEngineActivityStats.AddUnique(("Activity monitored", "unit", ""));
				lEngineActivityStats.Add(("Activity monitored", $"{engineName}", lItems.Count.ToString(_statsFormatCount)));

				//thread count
				lEngineActivityStats.AddUnique(("Thread count", "unit", ""));
				lEngineActivityStats.Add(("Thread count", $"{engineName}", first.threadPoolStatusThreadCount.ToString(_statsFormatCount)));

				//installed RAM
				lEngineActivityStats.AddUnique(("Installed Memory", "unit", "Gb"));
				lEngineActivityStats.Add(("Installed Memory", $"{engineName}", first.processInstalledMemoryGb.ToString(_statsFormatGb)));

				//CPU user time
				lEngineActivityStats.AddUnique(("CPU User Time at start", "unit", "seconds"));
				lEngineActivityStats.Add(("CPU User Time at start", $"{engineName}", first.processCPUUserTimeMs.ToString(_statsFormatCPU)));
				lEngineActivityStats.AddUnique(("CPU User Time at end", "unit", "seconds"));
				lEngineActivityStats.Add(("CPU User Time at end", $"{engineName}", last.processCPUUserTimeMs.ToString(_statsFormatCPU)));
				lEngineActivityStats.AddUnique(("CPU User Time change", "unit", "seconds"));
				lEngineActivityStats.Add(("CPU User Time change", $"{engineName}", (last.processCPUUserTimeMs - first.processCPUUserTimeMs).ToString(_statsFormatCPUChange)));

				//CPU system time
				lEngineActivityStats.AddUnique(("CPU System Time at start", "unit", "seconds"));
				lEngineActivityStats.Add(("CPU System Time at start", $"{engineName}", first.processCPUSystemTimeMs.ToString(_statsFormatCPU)));
				lEngineActivityStats.AddUnique(("CPU System Time at end", "unit", "seconds"));
				lEngineActivityStats.Add(("CPU System Time at end", $"{engineName}", last.processCPUSystemTimeMs.ToString(_statsFormatCPU)));
				lEngineActivityStats.AddUnique(("CPU System Time change", "unit", "seconds"));
				lEngineActivityStats.Add(("CPU System Time change", $"{engineName}", (last.processCPUSystemTimeMs - first.processCPUSystemTimeMs).ToString(_statsFormatCPUChange)));

				//process VM Size
				lEngineActivityStats.AddUnique(("VM Size at start", "unit", "Gb"));
				lEngineActivityStats.Add(("VM Size at start", $"{engineName}", first.processVMSizeGb.ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("VM Size at end", "unit", "Gb"));
				lEngineActivityStats.Add(("VM Size at end", $"{engineName}", last.processVMSizeGb.ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("VM Size change", "unit", "Gb"));
				lEngineActivityStats.Add(("VM Size change", $"{engineName}", (last.processVMSizeGb - first.processVMSizeGb).ToString(_statsFormatGbChange)));
				lEngineActivityStats.AddUnique(("VM Size min", "unit", "Gb"));
				lEngineActivityStats.Add(("VM Size min", $"{engineName}", lItems.Min(x => x.processVMSizeGb).ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("VM Size avg", "unit", "Gb"));
				lEngineActivityStats.Add(("VM Size avg", $"{engineName}", lItems.Average(x => x.processVMSizeGb).ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("VM Size max", "unit", "Gb"));
				lEngineActivityStats.Add(("VM Size max", $"{engineName}", lItems.Max(x => x.processVMSizeGb).ToString(_statsFormatGb)));

				//process WS Size
				lEngineActivityStats.AddUnique(("WS Size at start", "unit", "Gb"));
				lEngineActivityStats.Add(("WS Size at start", $"{engineName}", first.processWSSizeGb.ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("WS Size at end", "unit", "Gb"));
				lEngineActivityStats.Add(("WS Size at end", $"{engineName}", last.processWSSizeGb.ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("WS Size change", "unit", "Gb"));
				lEngineActivityStats.Add(("WS Size change", $"{engineName}", (last.processWSSizeGb - first.processWSSizeGb).ToString(_statsFormatGbChange)));
				lEngineActivityStats.AddUnique(("WS Size min", "unit", "Gb"));
				lEngineActivityStats.Add(("WS Size min", $"{engineName}", lItems.Min(x => x.processWSSizeGb).ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("WS Size avg", "unit", "Gb"));
				lEngineActivityStats.Add(("WS Size avg", $"{engineName}", lItems.Average(x => x.processWSSizeGb).ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("WS Size max", "unit", "Gb"));
				lEngineActivityStats.Add(("WS Size max", $"{engineName}", lItems.Max(x => x.processWSSizeGb).ToString(_statsFormatGb)));

				//Available Memory
				lEngineActivityStats.AddUnique(("Available Memory at start", "unit", "Gb"));
				lEngineActivityStats.Add(("Available Memory at start", $"{engineName}", first.processAvailableMemoryGb.ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("Available Memory at end", "unit", "Gb"));
				lEngineActivityStats.Add(("Available Memory at end", $"{engineName}", last.processAvailableMemoryGb.ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("Available Memory change", "unit", "Gb"));
				lEngineActivityStats.Add(("Available Memory change", $"{engineName}", (last.processAvailableMemoryGb - first.processAvailableMemoryGb).ToString(_statsFormatGbChange)));
				lEngineActivityStats.AddUnique(("Available Memory min", "unit", "Gb"));
				lEngineActivityStats.Add(("Available Memory min", $"{engineName}", lItems.Min(x => x.processAvailableMemoryGb).ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("Available Memory avg", "unit", "Gb"));
				lEngineActivityStats.Add(("Available Memory avg", $"{engineName}", lItems.Average(x => x.processAvailableMemoryGb).ToString(_statsFormatGb)));
				lEngineActivityStats.AddUnique(("Available Memory max", "unit", "Gb"));
				lEngineActivityStats.Add(("Available Memory max", $"{engineName}", lItems.Max(x => x.processAvailableMemoryGb).ToString(_statsFormatGb)));

				//Working threads
				lEngineActivityStats.AddUnique(("Working Threads at start", "unit", ""));
				lEngineActivityStats.Add(("Working Threads at start", $"{engineName}", first.threadPoolStatusWorkingThreads.ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Working Threads at end", "unit", ""));
				lEngineActivityStats.Add(("Working Threads at end", $"{engineName}", last.threadPoolStatusWorkingThreads.ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Working Threads change", "unit", ""));
				lEngineActivityStats.Add(("Working Threads change", $"{engineName}", (last.threadPoolStatusWorkingThreads - first.threadPoolStatusWorkingThreads).ToString(_statsFormatThreadsChange)));
				lEngineActivityStats.AddUnique(("Working Threads min", "unit", ""));
				lEngineActivityStats.Add(("Working Threads min", $"{engineName}", lItems.Min(x => x.threadPoolStatusWorkingThreads).ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Working Threads avg", "unit", ""));
				lEngineActivityStats.Add(("Working Threads avg", $"{engineName}", lItems.Average(x => x.threadPoolStatusWorkingThreads).ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Working Threads max", "unit", ""));
				lEngineActivityStats.Add(("Working Threads max", $"{engineName}", lItems.Max(x => x.threadPoolStatusWorkingThreads).ToString(_statsFormatThreads)));

				//idle threads
				lEngineActivityStats.AddUnique(("Idle Threads at start", "unit", ""));
				lEngineActivityStats.Add(("Idle Threads at start", $"{engineName}", first.threadPoolStatusIdleThreads.ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Idle Threads at end", "unit", ""));
				lEngineActivityStats.Add(("Idle Threads at end", $"{engineName}", last.threadPoolStatusIdleThreads.ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Idle Threads change", "unit", ""));
				lEngineActivityStats.Add(("Idle Threads change", $"{engineName}", (last.threadPoolStatusIdleThreads - first.threadPoolStatusIdleThreads).ToString(_statsFormatThreadsChange)));
				lEngineActivityStats.AddUnique(("Idle Threads min", "unit", ""));
				lEngineActivityStats.Add(("Idle Threads min", $"{engineName}", lItems.Min(x => x.threadPoolStatusIdleThreads).ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Idle Threads avg", "unit", ""));
				lEngineActivityStats.Add(("Idle Threads avg", $"{engineName}", lItems.Average(x => x.threadPoolStatusIdleThreads).ToString(_statsFormatThreads)));
				lEngineActivityStats.AddUnique(("Idle Threads max", "unit", ""));
				lEngineActivityStats.Add(("Idle Threads max", $"{engineName}", lItems.Max(x => x.threadPoolStatusIdleThreads).ToString(_statsFormatThreads)));
			}

			LogArray LAActivity = new LogArray(lEngineActivityStats);
			LAActivity.Log();

			Sys.Log($"----------------------------------------------------");
			swStats.Stop();
			Sys.Log($"Log Engine Activity Stats [{Sys.TimerGetText(swStats.ElapsedMilliseconds)}]");
			Sys.Log($"----------------------------------------------------");
		}
	}

	public class EngineActivityItem
	{
		//Engine
		public string engineName { get; }
		//Process
		public DateTime processNow { get; private set; }
		public double processCPUTimeMs { get; private set; }
		public double processCPUUserTimeMs { get; private set; }
		public double processCPUSystemTimeMs { get; private set; }
		public long processVMSizeKb { get; private set; }
		public long processWSSizeKb { get; private set; }
		public int processInstalledMemoryMb { get; private set; }
		public int processAvailableMemoryMb { get; private set; }
		//Activity
		public int activityConnectionsCount { get; private set; }
		public double activityQueriesAverageProcessingTimeMs { get; private set; }
		public double activityQueriesThroughput { get; private set; }
		public int activityOverload { get; private set; }
		public int activityIsRecoveringFromOverload { get; private set; }
		public int activityRefusesConnections { get; private set; }
		//ThreadPoolStatus
		public int threadPoolStatusThreadCount { get; private set; }
		public int threadPoolStatusWorkingThreads { get; private set; }
		public int threadPoolStatusIdleThreads { get; private set; }

		private double _MbToKb = 1024;
		private double _KbToGb = 1024 * 1024;

		public double processVMSizeGb { get { return Convert.ToDouble(this.processVMSizeKb) / _KbToGb; } }
		public double processWSSizeGb { get { return Convert.ToDouble(this.processWSSizeKb) / _KbToGb; } }
		public double processInstalledMemoryGb { get { return Convert.ToDouble(this.processInstalledMemoryMb) / _MbToKb; } }
		public double processAvailableMemoryGb { get { return Convert.ToDouble(this.processAvailableMemoryMb) / _MbToKb; } }

		public EngineActivityItem(string engineName)
		{
			this.engineName = engineName;
		}

		public bool Parse(string activityXML)
		{
			XmlDocument xml = new XmlDocument();
			xml.LoadXml(activityXML);

			//Process
			this.processNow = DateTime.ParseExact(xml.SelectSingleNode("/Engine/Process/Now").InnerText, "yyyy-MM-dd HH:mm:ss,fff", null);
			if (!double.TryParse(xml.SelectSingleNode("/Engine/Process/CPUTimeMs").InnerText, out double processCPUTimeMs)) return false;
			else this.processCPUTimeMs = processCPUTimeMs;
			if (!double.TryParse(xml.SelectSingleNode("/Engine/Process/CPUUserTimeMs").InnerText, out double processCPUUserTimeMs)) return false;
			else this.processCPUUserTimeMs = processCPUUserTimeMs;
			if (!double.TryParse(xml.SelectSingleNode("/Engine/Process/CPUSystemTimeMs").InnerText, out double processCPUSystemTimeMs)) return false;
			else this.processCPUSystemTimeMs = processCPUSystemTimeMs;
			if (!long.TryParse(xml.SelectSingleNode("/Engine/Process/VMSizeKb").InnerText, out long processVMSizeKb)) return false;
			else this.processVMSizeKb = processVMSizeKb;
			if (!long.TryParse(xml.SelectSingleNode("/Engine/Process/WSSizeKb").InnerText, out long processWSSizeKb)) return false;
			else this.processWSSizeKb = processWSSizeKb;
			if (!int.TryParse(xml.SelectSingleNode("/Engine/Process/InstalledMemoryMb").InnerText, out int processInstalledMemoryMb)) return false;
			else this.processInstalledMemoryMb = processInstalledMemoryMb;
			if (!int.TryParse(xml.SelectSingleNode("/Engine/Process/AvailableMemoryMb").InnerText, out int processAvailableMemoryMb)) return false;
			else this.processAvailableMemoryMb = processAvailableMemoryMb;

			//Activity
			//TODO - Connections Count - not used anymore ?
			if (!int.TryParse(xml.SelectSingleNode("/Engine/Activity/Connections/Count").InnerText, out int activityConnectionsCount)) return false;
			else this.activityConnectionsCount = activityConnectionsCount;
			if (!double.TryParse(xml.SelectSingleNode("/Engine/Activity/Queries/AverageProcessingTimeMs").InnerText, out double activityQueriesAverageProcessingTimeMs)) return false;
			else this.activityQueriesAverageProcessingTimeMs = activityQueriesAverageProcessingTimeMs;
			if (!double.TryParse(xml.SelectSingleNode("/Engine/Activity/Queries/Throughput").InnerText, out double activityQueriesThroughput)) return false;
			else this.activityQueriesThroughput = activityQueriesThroughput;
			if (!int.TryParse(xml.SelectSingleNode("/Engine/Activity/Overload").InnerText, out int activityOverload)) return false;
			else this.activityOverload = activityOverload;
			if (!int.TryParse(xml.SelectSingleNode("/Engine/Activity/IsRecoveringFromOverload").InnerText, out int activityIsRecoveringFromOverload)) return false;
			else this.activityIsRecoveringFromOverload = activityIsRecoveringFromOverload;
			//TODO - Refuses Connections - not used anymore ?
			if (!int.TryParse(xml.SelectSingleNode("/Engine/Activity/RefusesConnections").InnerText, out int activityRefusesConnections)) return false;
			else this.activityRefusesConnections = activityRefusesConnections;

			//ThreadPoolStatus
			if (!int.TryParse(xml.SelectSingleNode("/Engine/ThreadPoolStatus").Attributes.GetNamedItem("size").Value, out int threadPoolStatusThreadCount)) return false;
			else this.threadPoolStatusThreadCount = threadPoolStatusThreadCount;
			this.threadPoolStatusWorkingThreads = xml.SelectNodes("//Engine/ThreadPoolStatus/Thread/State[text() = 'working']").Count;
			this.threadPoolStatusIdleThreads = xml.SelectNodes("//Engine/ThreadPoolStatus/Thread/State[text() = 'idle']").Count;

			return true;
		}

		public void LogItem()
		{
			Sys.Log($"Engine [{engineName}] activity - Threads [{threadPoolStatusThreadCount}] Working [{threadPoolStatusWorkingThreads}] Idle [{threadPoolStatusIdleThreads}] - Overload [{activityOverload}] Is Recovering From Overload [{activityIsRecoveringFromOverload}] ");
		}

		public string ToCSVHeaders(char separator)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append("Engine" + separator);
			sb.Append("Process Time" + separator);
			sb.Append("Process CPUTime Ms" + separator);
			sb.Append("Process CPUUserTime Ms" + separator);
			sb.Append("Process CPUSystemTime Ms" + separator);
			sb.Append("Process VMSize Gb" + separator);
			sb.Append("Process WSSize Gb" + separator);
			sb.Append("Process InstalledMemory Gb" + separator);
			sb.Append("Process AvailableMemory Gb" + separator);

			sb.Append("Queries AverageProcessingTime Ms" + separator);
			sb.Append("Queries Throughput" + separator);

			sb.Append("Overload" + separator);
			sb.Append("IsRecoveringFromOverload" + separator);

			sb.Append("Threads Working" + separator);
			sb.Append("Threads Idle" + separator);
			return sb.ToString();
		}

		public string ToCSV(char separator)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(this.engineName + separator);
			sb.Append(this.processNow.ToString("yyyy-MM-dd HH:mm:ss,fff") + separator);
			sb.Append(this.processCPUTimeMs.ToString("0.#") + separator);
			sb.Append(this.processCPUUserTimeMs.ToString("0.#") + separator);
			sb.Append(this.processCPUSystemTimeMs.ToString("0.#") + separator);
			sb.Append(this.processVMSizeGb.ToString("0.##") + separator);
			sb.Append(this.processWSSizeGb.ToString("0.##") + separator);
			sb.Append(this.processInstalledMemoryGb.ToString("0.##") + separator);
			sb.Append(this.processAvailableMemoryGb.ToString("0.##") + separator);

			sb.Append(this.activityQueriesAverageProcessingTimeMs.ToString("0.#") + separator);
			sb.Append(this.activityQueriesThroughput.ToString("0.#") + separator);

			sb.Append(this.activityOverload.ToString() + separator);
			sb.Append(this.activityIsRecoveringFromOverload.ToString() + separator);

			sb.Append(this.threadPoolStatusWorkingThreads.ToString() + separator);
			sb.Append(this.threadPoolStatusIdleThreads.ToString() + separator);

			return sb.ToString();
		}
	}

}
