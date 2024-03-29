<img src="sinequa-logo-light-lg.png" width="300" style="margin: auto; display: block;">

# Plugin Repository

The goal of this repository is to host useful plugins developed in the frame of Sinequa projects, by Sinequa employees, partners or customers.

You are welcome to contribute to this repository via [Pull Requests](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/about-pull-requests). You may submit your Pull Requests from your own Fork of the repository, or by pushing new branches to this repository, if you are a member.

Each plugin must fulfill the following requirements:

- It must be packaged in its own subdirectory, along with its dependencies (forms, resource files, etc.), following if possible the standard structure of the Sinequa configuration folder.
- It should also include a detailed README file explaining how to use it.
- The code should be clean, readable and commented.
- The code should be parameterized rather than hard-coded. When possible, use a **form-override** to make it is easy to use the plugin from the administration.
- The code should be fully anonymized and free of project-specific or company data.

Please contact us if you have a doubt about how or what to contribute.

Unless otherwise mentioned in the directory of a specific plugin, all the plugins in this repository are released under the terms of the MIT License. The source code is provided as is, with no guarantee of correctness, performance, or future maintenance and compatibility.

## Table of content

- [PageRank Command](#pagerank-command)
- [Geo Search](#geo-search)
- [Engine Benchmark](#engine-benchmark)
- [Export Index to CSV](#export-index-to-csv)
- [Slack Connector](#slack-connector)
- [Export To Sharepoint](#export-to-sharepoint)
- [ConfigChecker](#configchecker)

### [PageRank Command](https://github.com/sinequa/plugins/tree/master/PageRank)

PageRank is an algorithm that aims to compute the importance of documents relative to each other within a corpus, independently from any user query. This plugin packages an open-source implementation of PageRank and allows to apply the algorithm on a Sinequa index, in order to improve the relevance of search results.

### [Geo Search](https://github.com/sinequa/plugins/tree/master/GeoSearch)

Sinequa can search within geolocated data by setting up an engine plugin, enabling powerful functionalities, like computing the intersection of complex geographical primitives. This plugin provides a set of Function plugins to generate geographical primitives _at indexing time_ and a sample Query plugin that allows searching geolocated documents or records _at query time_.

### [Engine Benchmark](https://github.com/sinequa/plugins/tree/master/EngineBenchmark)

EngineBenchmak command aims to measure engine(s) performances at query time. Using this command, you can run repeatable and quantifiable scenarios to analyze what are the performances of the engine(s) through a set of metrics.

### [Export Index to CSV](https://github.com/sinequa/plugins/tree/master/ExportIndexToCSV)

ExportIndexToCSV is a command that aims to simplify data export from your indexes. Unlike the ExecSqlCsv scmd command ExportIndexToCSV offer you additional settings to better control how data get exported.

### [Slack Connector](https://github.com/sinequa/plugins/tree/master/SlackConnector)

SlackConnector is an implementation of a generic connector with a ConnectorPlugin. This example consumes a REST API for indexing [Slack](https://www.slack.com).

### [Export To Sharepoint](https://github.com/sinequa/plugins/tree/master/ExportToSharepoint)

ExportToSharepoint command helps to export large files (>300 MB) to sharepoint.

### [ConfigChecker](https://github.com/sinequa/plugins/tree/master/ConfigChecker)

Many times configuration files need to be pushed from development environment to higher environments(like staging or production).
There are some good practices to be followed for configuration files when pushed to production, like force re-indexation should be set to false, loglevel should be set to default, indexer should be set to alias etc. ConfigChecker command helps to check sinequa configuration files to validate these element values.
