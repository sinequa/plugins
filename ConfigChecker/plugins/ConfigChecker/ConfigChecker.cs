///////////////////////////////////////////////////////////
// Plugin ConfigChecker : file ConfigChecker.cs
//

using System;
using System.Collections.Generic;
using System.Text;
using Sinequa.Common;
using Sinequa.Configuration;
using Sinequa.Plugins;
using Sinequa.Connectors;
using Sinequa.Indexer;
using Sinequa.Search;
using System.IO;
using System.Xml;

namespace Sinequa.Plugin
{
	public class ConfigChecker : CommandPlugin
	{
		/* Setting up the variables that will be used in the command. */
		private string source_path = "";
		private string logLevelCheck, indexerCheck, forceReindexCheck, securityCheck, customElementCheck, customElementName, customValue, indexerValue, securityValue,custom_ow_Value = "";
		private string rmlogLevel, rmForceReindex, rmIndexer, rmSecurity,rmCustom = "";
		private string logLevelNodeStr = "/Sinequa/System/LogLevel";
		private string indexerNodeStr1 = "/Sinequa/Indexers";
		private string indexerNodeStr2 = "/Sinequa/indexers";
		private string forceReIndexNodeStr = "/Sinequa/ForceReindexation";
		private string securityNodeStr1 = "/Sinequa/domain";
		private string securityNodeStr2 = "/Sinequa/Domain";


		public override Return OnPreExecute()
		{
			/* Getting the values from the command form. */
			source_path = Command.GetValue("CMD_SOURCEPATH");
			logLevelCheck = Command.GetValue("CMD_LOGLEVEL");
			indexerCheck = Command.GetValue("CMD_INDEXER");
			forceReindexCheck = Command.GetValue("CMD_FORCEREINDEX");
			securityCheck = Command.GetValue("CMD_SECURITY");
			customElementCheck = Command.GetValue("CMD_CUSTOM");
			customElementName = Command.GetValue("CMD_CUSTOMELEMENT");
			customValue = Command.GetValue("CMD_CUSTOMELEMENTVALUE");
			rmlogLevel = Command.GetValue("CMD_RMLOGLEVEL");
			rmForceReindex = Command.GetValue("CMD_RMFORCEREINDEX");
			rmIndexer = Command.GetValue("CMD_RMINDEXER");
			rmSecurity = Command.GetValue("CMD_RMSECURITY");
			rmCustom = Command.GetValue("CMD_RMCUSTOM");
			custom_ow_Value = Command.GetValue("CMD_CUSTOMVALUE");
			indexerValue = Command.GetValue("CMD_INDEXERVALUE");
			securityValue = Command.GetValue("CMD_SECURITYVALUE");

			Sys.Log("----------------------------Configuration Settings-------------------------");
			Sys.Log("SOURCE PATH: ", source_path);
			Sys.Log("LOG LEVEL CHECK: ", logLevelCheck);
			Sys.Log("INDEXER LEVEL CHECK: ", indexerCheck);
			Sys.Log("FORCE RE-INDEX CHECK: ", forceReindexCheck);
			Sys.Log("DOMAIN SECURITY CHECK: ", securityCheck);
			Sys.Log("CUSTOM ELEMENT CHECK: ", customElementCheck);
			Sys.Log("CUSTOM ELEMENT NAME: ", customElementName);
			Sys.Log("CUSTOM ELEMENT VALUE: ", customValue);
			Sys.Log("OVERWRITE LOG LEVEL: ", rmlogLevel);
			Sys.Log("OVERWRITE FORCE RE-INDEX: ", rmForceReindex);
			Sys.Log("OVERWRITE INDEXER: ", rmIndexer);
			Sys.Log("OVERWRITE INDEXER VALUE: ", indexerValue);
			Sys.Log("OVERWRITE SCURITY: ", rmSecurity);
			Sys.Log("OVERWRITE SECURITY VALUE: ", securityValue);
			Sys.Log("OVERWRITE CUSTOM ELEMENT: ", rmCustom);
			Sys.Log("OVERWRITE CUSTOM ELEMENT VALUE: ", custom_ow_Value);
			Sys.Log("---------------------------------------------------------------------------");

			return base.OnPreExecute();
		}

		
		public override Return OnExecute()
		
		{
			List<string> logLevelCheckList = new List<string>();
			List<string> indexerCheckList = new List<string>();
			List<string> forceReindexCheckList = new List<string>();
			List<string> securityCheckList = new List<string>();
			List<string> customCheckList = new List<string>();
			Dictionary<string, List<string>> summaryList = new Dictionary<string, List<string>>();

			try
			{
				string[] files = Directory.GetFiles(@source_path, "*.xml", SearchOption.AllDirectories);

				foreach (string file in files)
				{

					validateNode(logLevelCheck,logLevelCheckList,file,logLevelNodeStr,bool.Parse(rmlogLevel));
					validateNode(forceReindexCheck,forceReindexCheckList,file,forceReIndexNodeStr,"false",bool.Parse(rmForceReindex),"false");
					validateNode(indexerCheck,indexerCheckList,file,indexerNodeStr1,indexerNodeStr2,"@",bool.Parse(rmIndexer),indexerValue);
					validateNode(securityCheck,securityCheckList,file,securityNodeStr1,securityNodeStr2,"",bool.Parse(rmSecurity),securityValue);
					validateNode(customElementCheck,customCheckList,file,customElementName,customValue,bool.Parse(rmCustom),custom_ow_Value);
				}
				
				summaryList.Add("loglevel", logLevelCheckList);
				summaryList.Add("indexer", indexerCheckList);
				summaryList.Add("forcereindex", forceReindexCheckList);
				summaryList.Add("security", securityCheckList);
				summaryList.Add("custom", customCheckList);

				Sys.Log("------------------------------------Config Check List Summary-------------------------------");
				displaySummary(logLevelCheck,summaryList,"loglevel"," FILES WITH LOG LEVEL ");
				displaySummary(forceReindexCheck,summaryList,"forcereindex"," FILES WITH FORCE RE-INDEX ");
				displaySummary(indexerCheck,summaryList,"indexer"," FILES WITH NO INDEXER ALIAS ");				
				displaySummary(securityCheck,summaryList,"security"," FILES WITH NO SECURITY ");
				displaySummary(customElementCheck,summaryList,"custom"," FILES NOT MATCHING CUSTOM ELEMENT VALUE ");
			}

			catch (Exception e)
			{
				Sys.Log("Command Error :", e);
			}

			return base.OnExecute();
		}


		/// <summary>
		/// This function validates the node in the xml file
		/// </summary>
		/// <param name="isCheck">This is the value that you want to check for. If the value is found, then
		/// the node will be added to the nodeList.</param>
		/// <param name="nodeList">This is a list that is required for summary</param>
		/// <param name="file">The file to be validated</param>
		/// <param name="xpath">The xpath to the node you want to validate.</param>
		/// <param name="doOverwrite">If true, the value of the node will be overwritten with the value of
		/// overWriteValue.</param>
		/// <param name="overWriteValue">This is the value that will be written to the node</param>
		private void validateNode(string isCheck, List<string> nodeList, string file, string xpath, bool doOverwrite=false,string overWriteValue="")
		{
			try
			{
				XmlDocument xmlDoc = loadXML(file);
				XmlNode checkNode = xmlDoc.DocumentElement.SelectSingleNode(xpath);

				if(isCheck.Equals("true") & checkNode != null)
				{
					if(!string.IsNullOrEmpty(checkNode.InnerText))
					{
						nodeList.Add(file);
					}

					if(doOverwrite)
					{
						checkNode.InnerText = overWriteValue;
						xmlDoc.Save(file);
					}
				
				}
			}
			catch(Exception e )
			{
				Sys.Log("validate node Error ",e);
			}

		}


		/// <summary>
		/// This function validates the node value of a given XML file
		/// </summary>
		/// <param name="isCheck">This is the type of check you want to perform.  The options are:</param>
		/// <param name="nodeList">This is a list that is required for summary</param>
		/// <param name="file">The file to be validated</param>
		/// <param name="xpath">The xpath to the node you want to validate.</param>
		/// <param name="checkValue">The value that you want to check for.</param>
		/// <param name="doOverwrite">If the node exists, do you want to overwrite it?</param>
		/// <param name="overWriteValue">This is the value that will be written to the node</param>
		private void validateNode(string isCheck, List<string> nodeList, string file, string xpath,string checkValue,bool doOverwrite=false,string overWriteValue="")
		{
			try
			{
				XmlDocument xmlDoc = loadXML(file);
				XmlNode checkNode = xmlDoc.DocumentElement.SelectSingleNode(xpath);
				

				if(isCheck.Equals("true") & checkNode != null)
				{
					string nodeValue = checkNode.InnerText;
					if(!string.IsNullOrEmpty(checkNode.InnerText) & !nodeValue.Contains(checkValue))
					{
						nodeList.Add(file);
					}
				
				
				}
				if(checkNode != null)
				{
					if(doOverwrite)
					{
						checkNode.InnerText = overWriteValue;
						xmlDoc.Save(file);
					}
				}
			}
			catch(Exception e )
			{
				Sys.Log("validate node Error ",e);
			}

		}

  
		/// <summary>
		/// This function validates the nodeList against the file, xpath1, xpath2, checkValue, doOverwrite,
		/// and overWriteValue
		/// </summary>
		/// <param name="isCheck">This is the type of check you want to perform.  The options are:</param>
		/// <param name="nodeList">This is a list that is required for summary</param>
		/// <param name="file">The file to be validated</param>
		/// <param name="xpath1">The xpath to the node you want to check.</param>
		/// <param name="xpath2">This is the xpath to the node that you want to validate.</param>
		/// <param name="checkValue">The value to check for. If the value is not found, the node will be
		/// added.</param>
		/// <param name="doOverwrite">If true, the value of the node will be overwritten with the value of
		/// overWriteValue.</param>
		/// <param name="overWriteValue">This is the value that will be written to the node </param>
		private void validateNode(string isCheck, List<string> nodeList, string file, string xpath1, string xpath2, string checkValue=null,bool doOverwrite=false,string overWriteValue="")
		{
			try
			{
				XmlDocument xmlDoc = loadXML(file);
				XmlNode checkNode1 = xmlDoc.DocumentElement.SelectSingleNode(xpath1);
				XmlNode checkNode2 = xmlDoc.DocumentElement.SelectSingleNode(xpath2);
				string nodeValue = "";

				if (isCheck.Equals("true") & (checkNode1 != null | checkNode2 != null))
				{
					if (checkNode1 != null) 
					{ 
						nodeValue = checkNode1.InnerText; 
						checkNode1.InnerText = overWriteValue;
					}
					else 
					{ 
						nodeValue = checkNode2.InnerText; 
						checkNode2.InnerText = overWriteValue;
					}
					

					if (string.IsNullOrEmpty(nodeValue) )
					{
						
						nodeList.Add(file);
					}

					if( !string.IsNullOrEmpty(nodeValue) & !nodeValue.Contains(checkValue))
					{
						nodeList.Add(file);
					}
					
				}
				if(doOverwrite & (checkNode1 != null | checkNode2 != null))
				{
					
					xmlDoc.Save(file);
				}
			}
			catch(Exception e )
			{
				Sys.Log("validate node Error ",e);
			}
		}

		private XmlDocument loadXML(string file)
		{
			XmlDocument xmlDoc = new XmlDocument();
			xmlDoc.Load(file);
			return xmlDoc;
		}

  
		/// <summary>
		/// Displays the final summary
		/// </summary>
		/// <param name="isCheck">This is a string that is either "true" or "false". If it's "true", then the
		/// summary will be displayed. </param>
		/// <param name="summaryList">The dictionary that contains the summary information.</param>
		/// <param name="key">The key to the dictionary.</param>
		/// <param name="displayTitle">The title of the summary section.</param>
		private void displaySummary(string isCheck,Dictionary<string,List<string>> summaryList,string key, string displayTitle)
		{
			try
			{
				if(isCheck.Equals("true"))
				{
					Sys.Log("********************"+displayTitle+"**********************");
						if (summaryList[key].Count > 0)
						{
							summaryList[key].ForEach(x => Sys.Log(x));

						}
						else
						{
							Sys.Log("All OK!");
						}
						Sys.Log("********************************************************");
						Sys.Log("\n");
				}
			}
			catch(Exception e )
			{
				Sys.Log("display summary Error ",e);
			}

		}

	}
}
