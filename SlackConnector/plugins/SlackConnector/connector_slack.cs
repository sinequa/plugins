///////////////////////////////////////////////////////////
// Plugin Tests : file connector_slack.cs
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
//using Sinequa.Ml;
using System.Text.RegularExpressions;

namespace Sinequa.Plugin
{


    public class connector_slack : ConnectorPlugin
    {
        //Documentation of the Slack API: 
        //https://api.slack.com/authentication/basics
        //https://api.slack.com/web
        //https://api.slack.com/methods

        string url; // slack url
        string token;// Oauth Token 
        PatternMatcher channelMatcher;
        MapOf<JsonObject> users;
        bool indexMessages;
        bool indexFiles;

        #region common


        /// <summary>
        /// Read configuration (form) (partition & collection)
        /// </summary>
        /// <param name="root">Config</param>
        /// <returns>true if successfull, false for stopping the connector in error</returns>
        public override bool OnLoadConfig(XDoc root)
        {
            //initialize properties
            channelMatcher = new PatternMatcher("Channels", "Channels", 20);
            token = "";
            users = new MapOf<JsonObject>();

            //read configuration (collection & partition)
            url = Connector.CurrentConfig.Value("slack/url");
            token = Connector.CurrentConfig.Value("slack/token");
            channelMatcher.IncludedPattern.SetText(Connector.CurrentConfig.ValueList("slack/ChannelIncluded"));
            channelMatcher.ExcludedPattern.SetText(Connector.CurrentConfig.ValueList("slack/ChannelExcluded"));
            indexMessages = Connector.CurrentConfig.ValueBoo("slack/indexMessages", true);
            indexFiles = Connector.CurrentConfig.ValueBoo("slack/indexFiles", true);

            if (Str.IsEmpty(url) || Str.IsEmpty(token))
            {
                Sys.LogError("URL or Token is empty");
                return false;
            }

            //Notify that this connector supports the realtime mode
            Connector.RealTimeSupported = true;
            return true;
        }

        /// <summary>
        /// On starting the connector (after config loaded and all objects initilized)
        /// </summary>
        public override void OnConnectorStart()
        {
            //Add an authorization HTTP Header for All HTTP calls (of UrlAccess)
            Connector.UrlAccess.AddHttpHeader("Authorization", "Bearer " + token);


            // Need Enterprise Account
            //JsonObject response = slackGet("admin.teams.settings.info") as JsonObject;
            //Sys.Log(Json.Serialize(response));

        }

        #endregion


        #region utilities
        /// <summary>
        /// Do HTTP call with Sinequa HTTP requester
        /// </summary>
        /// <param name="api">API endpoint to use</param>
        /// <returns>a Json Object of the HTTP response</returns>
        protected Json slackGet(string api)
        {
            return Connector.UrlAccess.GetJson(Url.Add("https://slack.com/api/", api));
        }

        /// <summary>
        /// Replace slack markups <xxxx> by text 
        /// </summary>
        /// <param name="content">String content to modify</param>
        /// <param name="textOnly">Replace by links (false) or text only (true)</param>
        /// <returns>Content updated</returns>
        protected string transformText(string content, bool textOnly)
        {
            MatchCollection mc = Regex.Matches(content, "<([^>]+)>");
            if (mc.Count > 0)
            {
                foreach (Match m in mc)
                {
                    if (m.Groups.Count > 1)
                    {
                        string tag = m.Groups[1].Value;
                        if (Str.BeginWith(tag, "http")) //Eg. <http://www.sinequa.com>            
                            content = Str.Replace(content, "<" + tag + ">", textOnly ? tag : "<a href=\"" + tag + "\">" + tag + "</a>");
                        else if (Str.BeginWith(tag, "@")) //Eg. <@U7A9LH1GT>
                        {
                            string user = Str.RemoveBegin(tag, 1);
                            if (users.ContainsKey(user)) user = users[user].ValueStr("real_name");
                            content = Str.Replace(content, "<" + tag + ">", user);
                        }
                    }
                }
            }
            return content;
        }
        #endregion

        #region Collection
        /// <summary>
        /// Starting point of the Generic Connector for the collection
        /// </summary>
        /// <returns>true if succeded otherwise false</returns>
        public override bool OnGenericIndexCollection()
        {
            bool ok = true;
            users = new MapOf<JsonObject>();
            //put all users info in cache
            JsonObject response = slackGet("users.list") as JsonObject;
            if (response == null || (response != null && !response.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(response)); return false; }
            if (response != null)
            {
                JsonArray members = response.GetAsArray("members");
                if (members != null)
                {
                    for (int i = 0; i < members.EltCount(); i++)
                    {
                        JsonObject user = members.Elt(i) as JsonObject;
                        //Add user object in cache
                        users.Add(user.GetValue("id"), user);
                    }
                }
            }


            //index channels
            response = slackGet("conversations.list") as JsonObject;
            if (response == null || (response != null && !response.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(response)); return false; }
            if (response != null)
            {
                JsonArray channels = response.GetAsArray("channels");
                if (channels != null)
                {
                    for (int i = 0; i < channels.EltCount(); i++)
                    {
                        JsonObject channel = channels.Elt(i) as JsonObject;
                        string Name = channel.GetValue("name");
                        //check if included/excluded from conf
                        if (channelMatcher.IsExcluded(Name)) continue;

                        //Index the Channel
                        if (indexMessages) ok &= IndexChannelMessages(Name, channel);
                        if (indexFiles) ok &= IndexChannelFiles(Name, channel);
                    }
                }
            }
            return ok;
        }

        /// <summary>
        /// Index all messages of a channel
        /// </summary>
        /// <param name="channelName">Channel name for logging</param>
        /// <param name="channel">Json object which contains messages</param>
        /// <returns></returns>
        public bool IndexChannelMessages(string channelName, JsonObject channel)
        {
            Sys.Log("Index Messages of '" + channelName + "'");
            bool ok = true;
            SlackMessage doc = new SlackMessage();
            doc.cntr = this;
            if (Connector.IsModeRealTime()) Sys.Log("Index messages from date: " + Connector.RealTimeReferenceDate);
            string oldest = Connector.IsModeRealTime() ? Sys.ToStr(Dat.ToUnixTimestamp(Connector.RealTimeReferenceDate)) : "";
            string latest = "";
            bool has_more = false;
            do
            {
                //iterate on all messages by page (has_more = true)
                has_more = false;
                JsonObject response = slackGet("conversations.history?channel=" + channel.ValueStr("id") + "&latest=" + latest + "&oldest=" + oldest) as JsonObject;
                if (response == null || (response != null && !response.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(response)); return false; }
                if (response != null)
                {
                    has_more = response.ValueBoo("has_more", false);
                    JsonArray messages = response.GetAsArray("messages");
                    if (messages != null)
                    {
                        for (int i = 0; i < messages.EltCount(); i++)
                        {
                            //clear and reuse the doc instance 
                            doc.Clear();

                            //current message
                            JsonObject message = messages.Elt(i) as JsonObject;

                            //set custom to connectorDoc (slackDoc) 
                            doc.channel = channel;
                            doc.message = message;

                            //set mandatory fields: id, fileext & version
                            doc.Id = message.GetValue("user") + "-" + message.GetValue("ts");
                            doc.Version = message.GetValue("ts");
                            doc.FileExt = "htm";


                            //set latest for Slack API Pagination
                            latest = doc.Version;

                            //Process Doc (send to Indexer)
                            ok &= Connector.ProcessConnectorDoc(doc);
                        }
                    }
                }
            }
            while (has_more);
            return ok;
        }

        /// <summary>
        ///          Index all files of a channel
        /// </summary>
        /// <param name="channelName">Channel name for logging</param>
        /// <param name="channel">Json object which contains messages</param>
        /// <returns></returns>
        public bool IndexChannelFiles(string channelName, JsonObject channel)
        {
            Sys.Log("Index Files of '" + channelName + "'");
            bool ok = true;
            SlackFile doc = new SlackFile();
            doc.cntr = this;
            if (Connector.IsModeRealTime()) Sys.Log("Index files from date: " + Connector.RealTimeReferenceDate);
            string latest = Connector.IsModeRealTime() ? Sys.ToStr(Dat.ToUnixTimestamp(Connector.RealTimeReferenceDate)) : "";

            bool has_more = false;
            int page = 0;
            int pages = 1;
            do
            {
                //iterate on all files by page (has_more = true)
                has_more = false;
                page++;
                JsonObject response = slackGet("files.list?channel=" + channel.ValueStr("id") + "&ts_from=" + latest + "&page=" + page) as JsonObject;
                if (response == null || (response != null && !response.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(response)); return false; }
                if (response != null)
                {
                    pages = response.ValueInt("paging.pages");
                    has_more = response.ValueBoo("has_more", false);
                    JsonArray files = response.GetAsArray("files");
                    if (files != null)
                    {
                        for (int i = 0; i < files.EltCount(); i++)
                        {
                            //clear and reuse the doc instance 
                            doc.Clear();

                            //current message
                            JsonObject file = files.Elt(i) as JsonObject;

                            //set custom to connectorDoc (slackDoc) 
                            doc.channel = channel;
                            doc.file = file;

                            //set mandatory fields: id, fileext & version
                            doc.Id = file.GetValue("id");
                            doc.Version = file.GetValue("timestamp");
                            string mimetype = file.GetValue("mimetype");
                            if (Str.EQ(mimetype, "text/html"))
                                doc.FileExt = "htm";
                            else
                                doc.FileExt = Str.PathGetFileExt(file.GetValue("name"));


                            //set latest for Slack API Pagination
                            latest = doc.Version;

                            //Process Doc (send to Indexer)
                            ok &= Connector.ProcessConnectorDoc(doc);
                        }
                    }
                }
            }
            while (page < pages);
            return ok;
        }



        #endregion

        #region Partition
        /// <summary>
        /// Starting point for the generic connector in partition mode)
        /// </summary>
        /// <param name="partition">Connector instance</param>
        /// <returns></returns>
        public override bool OnGenericLoadPartition(ConnectorPartition partition)
        {
            //Sinequa object for a user/group
            ConnectorPrincipal p = new ConnectorPrincipal();

            //index users
            JsonObject response = slackGet("users.list") as JsonObject;
            if (response == null || (response != null && !response.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(response)); return false; }
            if (response != null)
            {
                JsonArray members = response.GetAsArray("members");
                if (members != null)
                {
                    for (int i = 0; i < members.EltCount(); i++)
                    {
                        //clear and reuse the principal instance 
                        p.Clear();

                        JsonDocProperties user = new JsonDocProperties(members.Elt(i));

                        //is a user
                        p.IsUser = true;


                        //principal fields
                        p.Id = user.GetValue("id");
                        p.Name = user.GetValue("name");
                        p.Fullname = user.GetValue("real_name");
                        p.Email = user.GetValue("profile.email");

                        Sys.Log2(10, "User: ", p.Name);
                        user.LogProperties();
                        //do partition mappings
                        Connector.PartitionMappings?.Apply(Ctxt, p, user);

                        //add principal in the partition
                        Connector.Partition.Write(p);
                    }
                }
            }

            //index groups
            response = slackGet("usergroups.list") as JsonObject;
            if (response == null || (response != null && !response.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(response)); return false; }
            if (response != null)
            {
                JsonArray members = response.GetAsArray("usergroups");
                if (members != null)
                {
                    for (int i = 0; i < members.EltCount(); i++)
                    {

                        //clear and reuse the principal instance 
                        p.Clear();

                        JsonDocProperties group = new JsonDocProperties(members.Elt(i));

                        //is a group
                        p.IsGroup = true;

                        //principal fields
                        p.Id = group.GetValue("id");
                        p.Name = group.GetValue("name");
                        JsonObject response2 = slackGet("usergroups.users.list?usergroup=" + p.Id) as JsonObject;
                        if (response2 != null)
                        {
                            //add memmbers of this group
                            p.Member = response.Get("users")?.ToListStr();
                        }

                        Sys.Log2(10, "group: ", p.Name);
                        group.LogProperties();
                        //do partition mappings
                        Connector.PartitionMappings?.Apply(Ctxt, p, group);

                        //add principal in the partition
                        Connector.Partition.Write(p);
                    }
                }
            }

            //index channels, see as groups
            response = slackGet("conversations.list") as JsonObject;
            if (response == null || (response != null && !response.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(response)); return false; }
            if (response != null)
            {
                JsonArray members = response.GetAsArray("channels");
                if (members != null)
                {
                    for (int i = 0; i < members.EltCount(); i++)
                    {
                        //clear and reuse the principal instance 
                        p.Clear();

                        JsonDocProperties channel = new JsonDocProperties(members.Elt(i));

                        //is a group
                        p.IsGroup = true;

                        //principal fields
                        p.Id = channel.GetValue("id");
                        p.Name = channel.GetValue("name");

                        //Get more details for having members (otherwise the list is truncated)
                        JsonObject responseInfo = slackGet("conversations.info?channel=" + p.Id) as JsonObject;
                        if (responseInfo == null || (responseInfo != null && !responseInfo.ValueBoo("ok"))) { Sys.LogError(Json.Serialize(responseInfo)); return false; }
                        if (responseInfo != null)
                        {
                            JsonObject channelinfo = responseInfo.GetAsObject("channel");
                            //add memmbers of this group
                            p.Member = channelinfo.Get("members")?.ToListStr();
                        }

                        Sys.Log2(10, "group (channel): ", p.Name);
                        channel.LogProperties();
                        //do partition mappings
                        Connector.PartitionMappings?.Apply(Ctxt, p, channel);

                        //add principal in the partition
                        Connector.Partition.Write(p);
                    }
                }
            }

            return true;
        }
        #endregion

        #region Connectordoc
        /// <summary>
        /// Slack Document for message
        /// </summary>
        public class SlackMessage : ConnectorDoc
        {

            //custom members
            public JsonObject message;
            public JsonObject channel;
            public connector_slack cntr;

            //clear custom objects
            public override void Clear()
            {
                base.Clear();
                message = null;
                channel = null;
            }

            /// <summary>
            /// Define accesslist of the document. It is the Channel id.
            /// </summary>
            /// <returns>True if succeded</returns>
            public override bool CalculateRights()
            {
                if (!Str.IsEmpty(Connector.DomainPrefix))
                {
                    AccessList1 = Str.And(Connector.DomainPrefix, channel.ValueStr("id"));
                }
                return true;
            }

            /// <summary>
            /// set all metadata for the document
            /// </summary>
            /// <returns>True if succeded</returns>
            public override bool LoadCompleteMetadatas()
            {
                //author id
                Authors = message.GetValue("user");
                //transform into a label (real_name)
                if (!Str.IsEmpty(Authors) && cntr.users.ContainsKey(Authors)) Authors = cntr.users[Authors].ValueStr("real_name");

                //folder for treepath column
                Folder = channel.ValueStr("name");

                //title
                Title = cntr.transformText(message.ValueStr("text"), true);

                //time in unixtime
                string ts = message.ValueStr("ts");
                Modified = Dat.FromUnixTimestamp(Sys.ToLng(Str.ParseToSep(ts, '.')));

                //url
                Url1 = Url.Add(cntr.url, "archives", channel.ValueStr("id"),  "p" + message.ValueStr("ts"));

                //docformat
                DocFormat = FileExt;

                //default properties
                return base.LoadCompleteMetadatas();
            }

            /// <summary>
            /// Define the fulltext content
            /// </summary>
            /// <returns></returns>
            public override Return LoadBlobDocument()
            {
                //set fulltext
                Blob = Fs.StrToBlob(cntr.transformText(message.ValueStr("text"), false));
                return base.LoadBlobDocument();
            }

            /// <summary>
            /// List all custom properties available for mappings
            /// </summary>
            /// <param name="l">List of properties</param>
            protected override void AddPropertyNamesToList(ListStr l)
            {
                //add custom properties (slack.channel.*) of channel for Log 20
                l.Add(channel.EltKeys(), "slack.channel.");

                //add custom properties (slack.message.*) of message for Log 20
                l.Add(message.EltKeys(), "slack.message.");

                //default properties
                base.AddPropertyNamesToList(l);
            }

            /// <summary>
            /// Returns a value for a given custom property
            /// </summary>
            /// <param name="name">Property name</param>
            /// <returns>Value of the property</returns>
            public override string GetValue(string name)
            {
                //get the value of a custom property 
                if (Str.BeginWith(name, "slack.channel."))
                    return channel.ValueStr(Str.ParseFromSep(name, "slack.channel."));
                //get the value of a custom property 
                if (Str.BeginWith(name, "slack.message."))
                    return message.ValueStr(Str.ParseFromSep(name, "slack.message."));
                //get the value of standard property 
                return base.GetValue(name);
            }
        }

        /// <summary>
        /// Slack Document for file
        /// </summary>
        public class SlackFile : ConnectorDoc
        {

            //custom members
            public JsonObject file;
            public JsonObject channel;
            public connector_slack cntr;

            //clear custom objects
            public override void Clear()
            {
                base.Clear();
                file = null;
                channel = null;
            }

            /// <summary>
            /// Define accesslist of the document. It is the Channel id.
            /// </summary>
            /// <returns></returns>
            public override bool CalculateRights()
            {
                if (!Str.IsEmpty(Connector.DomainPrefix))
                {
                    AccessList1 = file.Get("channels").ToListStr().ToStr(';', Connector.DomainPrefix);
                }
                return true;
            }

            /// <summary>
            /// set all metadata for the document
            /// </summary>
            /// <returns>True if succeded</returns>
            public override bool LoadCompleteMetadatas()
            {
                //author id
                Authors = file.GetValue("user");
                //transform into a label (real_name)
                if (!Str.IsEmpty(Authors) && cntr.users.ContainsKey(Authors)) Authors = cntr.users[Authors].ValueStr("real_name");

                //folder for treepath column
                Folder = channel.ValueStr("name");

                //filename
                FileName = file.ValueStr("name");

                //title
                Title = cntr.transformText(file.ValueStr("title"), true);

                //time in unixtime
                string ts = file.ValueStr("timestamp");
                Modified = Dat.FromUnixTimestamp(Sys.ToLng(ts));

                //url
                Url1 = file.ValueStr("permalink");

                //docformat
                DocFormat = FileExt;

                //default properties
                return base.LoadCompleteMetadatas();
            }

            /// <summary>
            /// Define the fulltext content
            /// </summary>
            /// <returns></returns>
            public override Return LoadBlobDocument()
            {
                string mimetype = file.GetValue("mimetype");
                string url = file.ValueStr("url_private_download");

                if (Str.EQNC(mimetype, "text/html"))
                {
                    Blob = Fs.StrToBlob(file.ValueStr("preview"));
                }
                else
                {
                    //set fulltext
                    Sys.Log("Download " + url);
                    Blob = Connector.UrlAccess.GetBlob(url);
                }
                return base.LoadBlobDocument();
            }

            /// <summary>
            /// List all custom properties available for mappings
            /// </summary>
            /// <param name="l">List of properties</param>
            protected override void AddPropertyNamesToList(ListStr l)
            {
                //add custom properties (slack.channel.*) of channel for Log 20
                l.Add(channel.EltKeys(), "slack.channel.");

                //add custom properties (slack.message.*) of message for Log 20
                l.Add(file.EltKeys(), "slack.file.");

                //default properties
                base.AddPropertyNamesToList(l);
            }

            /// <summary>
            /// Returns a value for a given custom property
            /// </summary>
            /// <param name="name">Property name</param>
            /// <returns>Value of the property</returns>
            public override string GetValue(string name)
            {
                //get the value of a custom property 
                if (Str.BeginWith(name, "slack.channel."))
                    return channel.ValueStr(Str.ParseFromSep(name, "slack.channel."));
                //get the value of a custom property 
                if (Str.BeginWith(name, "slack.file."))
                    return file.ValueStr(Str.ParseFromSep(name, "slack.file."));
                //get the value of standard property 
                return base.GetValue(name);
            }
        }
        #endregion

    }

}
