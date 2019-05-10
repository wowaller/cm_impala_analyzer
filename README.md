# cm_impala_analyzer
Project to collect Impala query information from CM using Java API and aggregate information for each job like memory, duration. Also provide advice on which resource pool to use for each job.

## Directory
* src: Source code for the project
* bin: Running script to launch the analyer.
* conf: Sample configuration.
* lib: Necessary libaray not on maven. Mostly Impala FE related package.

## How to compile
Maven is required. To compile:
* mvn package

## How to run
Using run.sh script to launch the job.
* run.sh <properties-file> <job-input-path> <result-path>

### Input properties:
* properties-file: Properties with job configuration.
* job-input-path: The input file with Job information.
* result-path: CSV format file to store the result.

### Properties
* cm_host: Host of Cloudera Manager.
* cm_port: Port of Cloudera Manager.
* cluster_name: Cluster name on CM. Normally cluster.
* service_name: Impala service name. Normally impala.
* username: CM user name to do the Impala SQL search.
* password: CM user password to do the Impala SQL search.
* api_version: Impala Api version. You can find it on CM API doc. For 5.15, it's v19.
* enable_ssl: True if CM has SSL enabled. Else set to false.
* ssl_pem_path: Path of CA cert for SSL.
* reader_class: Class used to read the job-input-path. For OM job, it's com.cloudera.sa.cm.OMTextTaskReader. Or you may implement one by you own by implementing the TaskReader interface.
* skip_header: Parameter in DefaultTaskReader and OMTextTaskReader. If skip the first line (header) of job input.
* found_only: Only output table if at least one SQL is found.
* all_source_only: Only output table if all source tables are found.
* start_time: ISO8601 format of start time to search the query. Example: 2019-04-27T16:27:24+0800
* end_time: ISO8601 format of end time to search the query. Example: 2019-04-28T16:27:24+0800
* filter: Filter to apply on search. Recommend only get finished jobs with a select statement like (queryState=FINISHED and (queryType = DDL or queryType = DML) and statement RLIKE ".\*select.\*")
* excludeKeyWords: List of key words delimitered by "," to ignore as source table. Sample: __m1903,__m1902
* excludeTbls: List of tables delimitered by "," to ignore as source table. Sample: public_base.pm00_base_operator,public_base.pm00_base_country

### Sample job input file
For now, two kinds of reader provided as 
* DefaultTaskReader: Input file contain lines as: ID \t target_table1,target_table2,... \t source_table1,source_table2,...
* OMTextTaskReader: Direct save of OM export excel to \t split text file.
