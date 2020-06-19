    ///////////////////////////////////////////////////////////
// Plugin FTT : file ExportIndexToCSV.cs
//

using System;
using System.Collections.Generic;
using System.Text;
using Sinequa.Common;
using Sinequa.Plugins;
using Sinequa.Search;
using System.Linq;
using System.IO;
using System.Security.Cryptography;
using Sinequa.Configuration;


namespace Sinequa.Plugin
{

    public class ExportIndexToCsv : CommandPlugin
    {

        CmdConfig conf;
        int logStatsEveryNRows = 10000;

        //Used to resolve value pattern
        //GetValue method is used to return column value using cursor
        private class DocExportIndexToCsv : IDocImpl
        {
            private readonly ExportIndexToCsv _cmd;
            private Engine.Client.Cursor _cursor;
            private Dictionary<string, string> _dColumnAlias = new Dictionary<string, string>();

            public DocExportIndexToCsv(ExportIndexToCsv cmd, Engine.Client.Cursor cursor, Dictionary<string, string> dColumnAlias)
            {
                _cmd = cmd;
                _cursor = cursor;
                _dColumnAlias = dColumnAlias;
            }

            public override string GetValue(string name)
            {
                if (_cursor != null)
                {
                    if (!_dColumnAlias.TryGetValue(name, out string columnName)) return Str.Empty;
                    var columnIndex = _cursor.GetColumnIndex(columnName);
                    if (columnIndex != -1)
                    {
                        return _cursor.GetColumn(columnIndex);
                    }
                }
                return Str.Empty;
            }
        }

        public override Return OnPreExecute()
        {
            conf = new CmdConfig(this.Command.Doc);
            if (!conf.LoadConfig()) return Return.Error;

            if (conf.simulate) Sys.Log("SIMULATE MODE");

            if (!conf.simulate)
            {   //try to create destination file
                if (!CanCreateFile()) return Return.Error;
            }

            return Return.Continue;
        }

        public override Return OnExecute()
        {
            string indexes;
            HashSet<string> hsMD5 = new HashSet<string>();
            int emptyLinesCount = 0;
            int duplicatesLinesCount = 0;
            int writtenLinesCount = 0;
            int rowProcessed = 0;
            ListStr lHeaders = new ListStr();

            EngineClient _client = null;
            Sinequa.Engine.Client.Cursor _cursor = null;

            //do not use StreamWriter in a using statement. In simulate mode the file is not created so this will trigger an exception. 
            StreamWriter sw = null;

            try
            {
                _client = EngineClientsPool.FromPoolByIndexList(conf.listIndexes, out indexes, conf.engine, false);
            }
            catch (Exception ex)
            {
                Sys.LogError("Cannot get Engine Client from pool for indexes [" + conf.listIndexes + "]");
                Sys.LogError(ex);
                return Return.Error;
            }
            Sys.Log("Using Engine client  [" + _client.Name + "]");

            try
            {

                Sys.Log("Execute query [" + conf.GetSQL() + "]");

                _cursor = _client.ExecCursor(conf.GetSQL());

                if (_cursor == null)
                {
                    Sys.LogError("Cannot get cursor for query [" + conf.GetSQL() + "]");
                    return Return.Error;
                }

                DocExportIndexToCsv doc = new DocExportIndexToCsv(this, _cursor, conf.dColumnColumnAlias);
                var context = new IDocContext { Doc = doc };

                Sys.Log("Query processingtime [" + _cursor.GetAttribute("processingtime") + "]");
                Sys.Log("Query row count [" + _cursor.TotalRowCount + "]");
                int globalTimer = Sys.TimerStart();

                if (!conf.simulate) {
                    sw = new StreamWriter(conf.destinationFilePath, false, Encoding.UTF8);
                }
                
                int localTimer = Sys.TimerStart();
                while (!_cursor.End())
                {
                    rowProcessed++;
                    if (rowProcessed % logStatsEveryNRows == 0)
                    {
                        Sys.Log("----------------------------------------------------");
                        Sys.Log("Number of rows processed [" + rowProcessed + "] ");
                        Sys.Log("Number of lines exported [" + writtenLinesCount + "] ");
                        Sys.Log("Number of empty lines removed [" + emptyLinesCount + "] ");
                        Sys.Log("Number of duplicated lines removed [" + duplicatesLinesCount + "] ");
                        Sys.Log("Processing [" + logStatsEveryNRows + "] rows in [", Sys.TimerGetText(Sys.TickDuration(localTimer)), "]");
                        localTimer = Sys.TimerStart();
                    }

                    ListStr l = new ListStr();
                    bool isEmpty = true;

                    for (int i = 0; i < _cursor.ColumnCount; i++)
                    {
                        if (conf.addHeaders && rowProcessed == 1)   //headers
                        {
                            string header = _cursor.GetColumnName(i);
                            if (conf.useDblQuote) header = "\"" + header + "\"";
                            lHeaders.Add(header);
                        }

                        string colValue = Str.Empty;
                        //cursor column match column mapping column name ?
                        if (conf.lColumnMapping.Exists(x => Str.EQNC(x.columnName, _cursor.GetColumnName(i)))) {
                            //get all matching column mapping for current column
                            List<ColumnMapping> lColumnMapping = conf.lColumnMapping.FindAll(x => Str.EQNC(x.columnName, _cursor.GetColumnName(i)));
                            foreach (ColumnMapping columnMapping in lColumnMapping)
                            {
                                if (columnMapping.slectionQuery.IsSelected(context, doc))  //match selection query ? if so, apply value pattern
                                {
                                    Sys.Log2(40, "Column [" + columnMapping.columnName + "] match selection query [" + columnMapping.slectionQuery.Sql + "]");

                                    colValue = IDocHelper.GetValue(context, doc, columnMapping.valueExpression);
                                    Sys.Log2(40, "Column [" + columnMapping.columnName + "] use value pattern [" + columnMapping.valueExpression + "] = [" + colValue + "]");
                                    break;  //stop mapping when selection query match
                                }
                                else
                                {
                                    Sys.Log2(40, "Column [" + columnMapping.columnName + "] don't match selection query [" + columnMapping.slectionQuery.Sql + "]");
                                    continue;   //go to next expression
                                }
                            }
                        }
                        //no column mapping, get value from cursor
                        else
                        {
                            colValue = _cursor.GetColumn(i);
                        }
                        if (!Str.IsEmpty(colValue)) isEmpty = false;
                        if (conf.useReplaceSeparator) colValue = colValue.Replace(conf.separator, conf.replaceSeparator);  //replace separator in values
                        if (conf.useDblQuote) colValue = "\"" + colValue + "\"";    //use double quote

                        l.Add(colValue);
                    }

                    string line = l.ToStr(conf.separator);

                    if (conf.removeDuplicates)  //remove duplicates
                    {    
                        string MD5 = Str.Md5(line);
                        if (!hsMD5.Add(MD5))
                        {
                            duplicatesLinesCount++;
                            _cursor.MoveNext();
                            continue;
                        }
                    }

                    if (conf.removeEmptyLines && isEmpty)   //remove empty lines
                    {
                        emptyLinesCount++;
                        _cursor.MoveNext();
                        continue;
                    }

                    writtenLinesCount++;
                    if (conf.simulate)  //simulate, add headers and line into logs
                    {
                        if (conf.addHeaders && rowProcessed == 1) Sys.Log(GetHeaders(lHeaders));  // write headers
                        Sys.Log(line);  //write line
                        if (writtenLinesCount >= conf.simulateCount) break;
                    }
                    else
                    {
                        if (conf.addHeaders && rowProcessed == 1) sw.WriteLine(GetHeaders(lHeaders)); // write headers
                        sw.WriteLine(line); //write line

                    }

                    _cursor.MoveNext();
                }

                if (sw != null) sw.Close(); //dispose stream writer

                
                Sys.Log("----------------------------------------------------");
                if (conf.removeEmptyLines) Sys.Log("Number of empty lines removed [" + emptyLinesCount + "]");
                if (conf.removeDuplicates) Sys.Log("Number of duplicated lines removed [" + duplicatesLinesCount + "]");
                Sys.Log("[" + writtenLinesCount + "] lines exported into file [" + conf.destinationFilePath + "]");
                Sys.Log("Processing [" + rowProcessed + "] rows in [", Sys.TimerGetText(Sys.TickDuration(globalTimer)), "]");
            }
            catch (Exception ex)
            {
                Sys.LogError("Select index Cursor error : ", ex);
                try
                {
                    if (_client != null) EngineClientsPool.ToPool(_client);
                }
                catch (Exception exe)
                {
                    Sys.LogError("EngineClientsPool ToPool : ", exe);
                    Sys.Log(exe.StackTrace);
                }
            }
            finally
            {
                try
                {
                    if (_cursor != null) _cursor.Close();
                }
                catch (Exception ex)
                {
                    Sys.LogError("Close cursor error : ", ex);
                    Sys.Log(ex.StackTrace);
                }
                EngineClientsPool.ToPool(_client);
            }

            return base.OnExecute();
        }

        public bool CanCreateFile()
        {
            if (!Fs.DirCreateRecursiveForFile(conf.destinationFilePath))
            {
                Sys.LogError("Cannot create file in [" + conf.destinationFilePath + "]");
                return false;
            }
            return true;
        }

        public string GetHeaders(ListStr lHeaders)
        {
            if(conf.overrideHeaders)
            {
                return conf.lCustomHeaders.ToStr(conf.separator);
            }
            return lHeaders.ToStr(conf.separator);
        }
    }

    public class CmdConfig
    {

        public bool addHeaders { get; private set; }
        public bool overrideHeaders { get; private set; }
        public ListStr lCustomHeaders { get; private set; } = new ListStr();
        public string listIndexes { get; private set; }
        public Dictionary<string, string> dColumnColumnAlias { get; private set; } = new Dictionary<string, string>();
        public string whereCluase { get; private set; }
        public string grouBy { get; private set; }
        public string orderBy { get; private set; }
        public int count { get; private set; }

        public string engine { get; private set; }

        public char separator { get; private set; }
        public bool useReplaceSeparator { get; private set; }
        public char replaceSeparator { get; private set; }
        public bool useDblQuote { get; private set; }
        public string destinationFilePath { get; private set; } = Str.Empty;
        public bool removeDuplicates { get; private set; }
        public bool removeEmptyLines { get; private set; }

        public bool simulate { get; private set; }
        public int simulateCount { get; private set; }

        public bool isAuditIndex { get; private set; } = false;

        public List<ColumnMapping> lColumnMapping { get; private set; } = new List<ColumnMapping>();

        private XDoc _XMLConf = null;
        private IDocContext _ctxt = new IDocContext();

        public CmdConfig(XDoc conf)
        {
            _XMLConf = conf;
            _ctxt.Doc = new IDocImpl();
        }

        public bool LoadConfig()
        {
            string value = Str.Empty;
            string dataTag = Str.Empty;

            Sys.Log2(20, "----------------------------------------------------");
            Sys.Log2(20, "Load configuration");
            Sys.Log2(20, "----------------------------------------------------");

            if (_XMLConf == null)
            {
                Sys.LogError("Cannot read configuration");
                return false;
            }

            //index list
            dataTag = "CMD_INDEX_LIST";
            if (!DatatagExist(dataTag)) return false;
            listIndexes = CC.Current.Indexes.CleanAliasList(_XMLConf.Value(dataTag, Str.Empty));
            if (String.IsNullOrEmpty(listIndexes))
            {
                Sys.LogError("Invalid configuration property: index list is empty");
                return false;
            }

            //list columns
            dataTag = "CMD_SELECT";
            if (!DatatagExist(dataTag)) return false;
            value = _XMLConf.Value(dataTag, "*");

            //if only one index and index type is audit => do not resolve column aliases
            CCIndex idx = CC.Current.Indexes.Get(listIndexes);
            isAuditIndex = idx != null && (idx.IndexType == CCIndexType.Audit || idx.IndexType == CCIndexType.AuditReplicated) ? true : false;
            ListStr lcolunmOrColumnAlias = ListStr.ListFromStr(value, ',');
            foreach (string s in lcolunmOrColumnAlias)
            {
                string colunmOrColumnAlias = s.Trim();
                if (!isAuditIndex)
                {
                    dColumnColumnAlias.Add(colunmOrColumnAlias, CC.Current.Global.ResolveColumn(colunmOrColumnAlias));
                }
                else dColumnColumnAlias.Add(colunmOrColumnAlias, colunmOrColumnAlias);
            }

            //where clause
            dataTag = "CMD_WHERE";
            if (!DatatagExist(dataTag)) return false;
            whereCluase = _XMLConf.Value(dataTag, "");
            whereCluase = IDocHelper.GetValuePattern(_ctxt, whereCluase);

            //group by
            dataTag = "CMD_GROUPBY";
            if (!DatatagExist(dataTag)) return false;
            grouBy = _XMLConf.Value(dataTag, "");

            //order by
            dataTag = "CMD_ORDERBY";
            if (!DatatagExist(dataTag)) return false;
            orderBy = _XMLConf.Value(dataTag, "");

            //count
            dataTag = "CMD_COUNT";
            if (!DatatagExist(dataTag)) return false;
            count = _XMLConf.ValueInt(dataTag, -1);

            //Engine
            dataTag = "CMD_ENGINE";
            if (!DatatagExist(dataTag)) return false;
            engine = CC.Current.Engines.CleanAlias(_XMLConf.Value(dataTag, ""));

            //add headers
            dataTag = "CMD_HEADERS";
            if (!DatatagExist(dataTag)) return false;
            addHeaders = _XMLConf.ValueBoo(dataTag, false);

            //Field separator
            dataTag = "CMD_SEPARATOR";
            if (!DatatagExist(dataTag)) return false;
            separator = _XMLConf.ValueChar(dataTag, ',');

            //override headers
            dataTag = "CMD_HEADERS_OVERRIDE";
            if (!DatatagExist(dataTag)) return false;
            overrideHeaders = _XMLConf.ValueBoo(dataTag, false);

            //custom headers
            dataTag = "CMD_HEADERS_CUSTOM";
            if (!DatatagExist(dataTag)) return false;
            string sCustomHeaders = _XMLConf.Value(dataTag, "");
            if (addHeaders && overrideHeaders) //check override header has same number of elements than select statement
            {
                lCustomHeaders = ListStr.ListFromStr2(sCustomHeaders, separator);
                if (lCustomHeaders.Count != dColumnColumnAlias.Count)
                {
                    Sys.LogError("Override headers does not have the same number of elements as select statement");
                    return false;
                }
            }

            //use replace separator
            dataTag = "CMD_USE_REPLACE";
            if (!DatatagExist(dataTag)) return false;
            useReplaceSeparator = _XMLConf.ValueBoo(dataTag, false);

            //Replace separator in values
            dataTag = "CMD_SEPARATOR_REPLACE";
            if (!DatatagExist(dataTag)) return false;
            replaceSeparator = _XMLConf.ValueChar(dataTag, '/');

            //Enclose fields in double quote
            dataTag = "CMD_DBL_QUOTES";
            if (!DatatagExist(dataTag)) return false;
            useDblQuote = _XMLConf.ValueBoo(dataTag, false);

            //Destination file path
            dataTag = "CMD_FILE_PATH";
            if (!DatatagExist(dataTag)) return false;
            destinationFilePath = CC.Current.EnvVars.Resolve(_XMLConf.Value(dataTag, ""));
            destinationFilePath = IDocHelper.GetValuePattern(_ctxt, destinationFilePath);
            if (Str.IsEmpty(destinationFilePath))
            {
                Sys.LogError("Export file path is empty");
                return false;
            }

            //Remove duplicate lines
            dataTag = "CMD_DEDUPLICATE";
            if (!DatatagExist(dataTag)) return false;
            removeDuplicates = _XMLConf.ValueBoo(dataTag, false);

            //Remove empty lines
            dataTag = "CMD_REMOVE_EMPTY_LINES";
            if (!DatatagExist(dataTag)) return false;
            removeEmptyLines = _XMLConf.ValueBoo(dataTag, false);

            //Enable simulate mode
            dataTag = "CMD_SIMULATE";
            if (!DatatagExist(dataTag)) return false;
            simulate = _XMLConf.ValueBoo(dataTag, false);

            //Simulate count
            dataTag = "CMD_NB_LINE_SIMULATE";
            if (!DatatagExist(dataTag)) return false;
            simulateCount = _XMLConf.ValueInt(dataTag, 1000);

            //Mapping Grid - do not check if DatatagExist - grid is optionnal
            dataTag = "CMD_MAPPING";
            ListOf<XDoc> mappingGridElts = _XMLConf.EltList("CMD_MAPPING");

            for (int i = 0; i < mappingGridElts.Count; i++)
            {
                XDoc mappingElt = mappingGridElts.Get(i);
                string columnOrAliasColumn = mappingElt.Value("Column");
                if (String.IsNullOrEmpty(columnOrAliasColumn) || String.IsNullOrWhiteSpace(columnOrAliasColumn))
                {
                    Sys.LogError("Column Mapping - Column cannot be empty");
                    return false;
                }

                string val = mappingElt.Value("Value");
                if (String.IsNullOrEmpty(val) || String.IsNullOrWhiteSpace(val))
                {
                    Sys.LogError("Column Mapping - Value Pattern cannot be empty for column [" + columnOrAliasColumn + "]");
                    return false;
                }

                string squery = mappingElt.Value("SelectionQuery");
                SelectionQuery selectionQuery = SelectionQuery.FromStr(squery, out string errorMessage);
                if (!Str.IsEmpty(errorMessage))
                {
                    Sys.LogError("Column Mapping - Invalid selection query [" + squery + "] for column [" + columnOrAliasColumn + "] - ", errorMessage);
                    return false;
                }

                if(!dColumnColumnAlias.TryGetValue(columnOrAliasColumn, out string columnName))
                {
                    columnName = null;
                }
                ColumnMapping columnMapping = new ColumnMapping(columnName, columnOrAliasColumn, val, selectionQuery);
                lColumnMapping.AddUnique(columnMapping);
            }

            Sys.Log2(20, "Load configuration OK");
            LogConfig();

            return true;
        }

        public void LogConfig()
        {
            Sys.Log("----------------------------------------------------");
            Sys.Log("Select : [" + string.Join(",", this.dColumnColumnAlias.Keys) + "]");
            Sys.Log("Index list : [" + this.listIndexes + "]");
            Sys.Log("Where clause : [" + this.whereCluase + "]");
            Sys.Log("Group by : [" + this.grouBy + "]");
            Sys.Log("Order by : [" + this.orderBy + "]");
            Sys.Log("Count : [" + this.count + "]");
            Sys.Log("----------------------------------------------------");
            Sys.Log("Engine: [" + this.engine + "]");
            Sys.Log("----------------------------------------------------");
            Sys.Log("Add headers : [" + this.addHeaders + "]");
            if (this.addHeaders) {
                Sys.Log("Override headers : [" + this.overrideHeaders + "]");
                if (this.overrideHeaders) {
                    Sys.Log("Custom headers : [" + this.lCustomHeaders.ToStr(this.separator) + "]");
                }
            }
            Sys.Log("Field separator : [" + this.separator + "]");
            Sys.Log("Enable replace separator in values : [" + this.useReplaceSeparator + "]");
            Sys.Log("Replace separator in values : [" + this.replaceSeparator + "]");
            Sys.Log("Enclose fields in double quote : [" + this.useDblQuote + "]");
            Sys.Log("Destination file path : [" + this.destinationFilePath + "]");
            Sys.Log("Remove duplicate lines : [" + this.removeDuplicates + "]");
            Sys.Log("Remove empty lines : [" + this.removeEmptyLines + "]");
            Sys.Log("----------------------------------------------------");
            Sys.Log("Enable simulate mode : [" + this.simulate + "]");
            Sys.Log("Number of lines to log in simulation mode : [" + this.simulateCount + "]");
            Sys.Log("----------------------------------------------------");
            Sys.Log("Using SQL : [" + this.GetSQL() + "]");
            Sys.Log("----------------------------------------------------");
            if (lColumnMapping.Count > 0)
            {
                foreach(ColumnMapping columnMapping in this.lColumnMapping)
                {
                    Sys.Log("Column Mapping : " + columnMapping.ToString());
                }
                Sys.Log("----------------------------------------------------");
            }
            if (isAuditIndex)
            {
                Sys.Log("Info: Index type [Audit], column aliases will not be resolved");
                Sys.Log("----------------------------------------------------");
            }
            
        }

        public string GetSQL()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT " + string.Join(",", this.dColumnColumnAlias.Values) + " ");
            sb.Append("FROM " + listIndexes + " ");
            if (!Str.IsEmpty(whereCluase)) sb.Append("WHERE " + whereCluase + " ");
            if (!Str.IsEmpty(grouBy)) sb.Append("GROUP BY " + grouBy + " ");
            if (!Str.IsEmpty(orderBy)) sb.Append("ORDER BY " + orderBy + " ");
            if (count > 0) sb.Append("SKIP 0 COUNT " + count);
            return sb.ToString();
        }

        public bool SQLIsValid()
        {
            if (dColumnColumnAlias == null || dColumnColumnAlias.Count == 0) return false;
            if (String.IsNullOrEmpty(listIndexes)) return false;
            return true;
        }

        private bool DatatagExist(string dataTag)
        {
            if (!_XMLConf.EltExist(dataTag))
            {
                Sys.LogError("Invalid configuration property, datatag [", dataTag, "] is missing");
                return false;
            }
            return true;
        }
    }

    public class ColumnMapping
    {
        public string columnName;
        public string columnOrAliasColumn;
        public string valueExpression;
        public SelectionQuery slectionQuery;

        public ColumnMapping(string columnName, string columnOrAliasColumn, string valueExpression, SelectionQuery slectionQuery)
        {
            this.columnName = columnName;
            this.columnOrAliasColumn = columnOrAliasColumn;
            this.valueExpression = valueExpression;
            this.slectionQuery = slectionQuery;
        }

        public override string ToString()
        {
            return "Column [" + this.columnName + "] Value Pattern [" + this.valueExpression + "] Selection Query [" + this.slectionQuery.Sql + "]";
        }
    }

    public class ExportIndexToCSV_SQL : WebFormControlPlugin
    {
        CmdConfig commadConfig;

        public override void GenerateHtml(string name, XDoc config)
        {

            commadConfig = new CmdConfig(this.Form.Data);
            if (commadConfig == null)
            {
                DisplayError("Cannot init Web Form Control");
            }
            commadConfig.LoadConfig();

            if (!commadConfig.SQLIsValid())
            {
                DisplayError("Invalid SQL query");
            }
            else
            {
                DisplaySQL();
            }
        }

        private void DisplayError(string errorMessage)
        {
            Hm.Write("<div style='width=100%; color:red;'>" + errorMessage + "</div>");
        }

        private void DisplaySQL()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("<div style='width=100%'><b>SQL query:</b> " + commadConfig.GetSQL() + "</div>");
            Hm.Write(sb.ToString());
        }
    }

}
