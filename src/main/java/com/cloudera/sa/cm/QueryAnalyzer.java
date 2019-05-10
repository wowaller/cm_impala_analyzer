package com.cloudera.sa.cm;

import com.cloudera.api.swagger.model.ApiImpalaQuery;
import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Main class to performance the analyze.
 */
public class QueryAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryAnalyzer.class);

    //configurations from input file.

    // CM host.
    public static final String CM_HOST = "cm_host";
    // CM port.
    public static final String CM_PORT = "cm_port";
    // Cluster name on CM.
    public static final String CLUSTER_NAME = "cluster_name";
    // Service name on CM.
    public static final String SERVICE_NAME = "service_name";
    // Username for CM query.
    public static final String USERNAME = "username";
    // Password for CM query.
    public static final String PASSWORD = "password";
    // API version for CM API.
    public static final String API_VERSION = "api_version";

    // Is SSL enabled on CM HTTP.
    public static final String ENABLE_SSL = "enable_ssl";
    public static final String DEFAULT_ENABLE_SSL = "false";
    // CA cert path if SSL enabled.
    public static final String PEM_PATH = "ssl_pem_path";
    public static final String DEFAULT_PEM_PATH = "";

    // Start time of searched query.
    public static final String QUERY_START_TIME = "start_time";
    // End time of searched query.
    public static final String QUERY_END_TIME = "end_time";
    // Filter to perform the CM query.
    public static final String QUERY_FILTER = "filter";
    public static final String DEFAULT_QUERY_FILTER = "";
    // Tables not search. Normally dimensional tables.
    public static final String EXCLUDE_TBL_LIST = "excludeTbls";
    public static final String EXCLUDE_KEY_LIST = "excludeKeyWords";
    public static final String DEFAULT_LIST_DELIMITER = ",";
    // Input source table has DB information or not. Just in case there's no information for DB in source table.
    public static final String IGNORE_DB_NAME = "ignore_db";
    public static final String DEFAULT_IGNORE_DB_NAME = "false";
    // Resource pool configuration to provide recommendation. Format: pool1:memlimit1,pool2:memlimit2,...
    public static final String IMPALA_RESOURCE_POOL_LIST = "resource_pool";
    // Output information of only found jobs. Do not output those that has no SQL found.
    public static final String FOUND_TASK_ONLY = "found_only";
    public static final String DEFAULT_FOUND_TASK_ONLY = "false";
    // Output information only if all source tables found.
    public static final String ALL_SOURCE_FOUND = "all_source_only";
    public static final String DEFAULT_ALL_SOURCE_FOUND = "false";

    public static final String DEFAULT_INPUT_SPLIT = "\\|";

    private String host;
    private int port;
    private String version;
    private String username;
    private String password;

    private Boolean isSSLEnabled;
    private String pemPath;

    private ImpalaQuerySearch client;

    private String clusterName;
    private String serviceName;
    private String from;
    private String to;
    private String filter;
    private TaskReader reader;
    private boolean ignoreDB;
    private boolean outputFoundOnly;
    private boolean allSrcFoundOnly;

    private Map<String, QueryBase> allQueries;

    private Set<String> excludeTbls;
    private Set<String> excludeKeys;

    private Map<String, Double> queueSetting;

    public QueryAnalyzer(String input, Properties props) throws ClassNotFoundException, IOException, InstantiationException, IllegalAccessException {
        host = props.getProperty(CM_HOST);
        port = Integer.parseInt(props.getProperty(CM_PORT));
        version = props.getProperty(API_VERSION);
        username = props.getProperty(USERNAME);
        password = props.getProperty(PASSWORD);

        isSSLEnabled = Boolean.parseBoolean(props.getProperty(ENABLE_SSL, DEFAULT_ENABLE_SSL));
        pemPath = props.getProperty(PEM_PATH, DEFAULT_PEM_PATH);

        client = new ImpalaQuerySearch(host, port, version, username, password, isSSLEnabled, pemPath);

        clusterName = props.getProperty(CLUSTER_NAME);
        serviceName = props.getProperty(SERVICE_NAME);
        from = props.getProperty(QUERY_START_TIME);
        to = props.getProperty(QUERY_END_TIME);
        filter = props.getProperty(QUERY_FILTER, DEFAULT_QUERY_FILTER);
        String excludeString = props.getProperty(EXCLUDE_TBL_LIST);

        allQueries = new HashMap<>();
        excludeTbls = new HashSet<>();
        if(excludeString != null) {
            excludeTbls.addAll(Arrays.asList(excludeString.split(DEFAULT_LIST_DELIMITER)));
        }

        String excludeKeyString = props.getProperty(EXCLUDE_KEY_LIST);
        excludeKeys = new HashSet<>();
        if(excludeKeyString != null) {
            excludeKeys.addAll(Arrays.asList(excludeKeyString.split(DEFAULT_LIST_DELIMITER)));
        }

        reader = TaskReaderFactory.getReader(input, props);
        ignoreDB = Boolean.parseBoolean(props.getProperty(IGNORE_DB_NAME, DEFAULT_IGNORE_DB_NAME));
        outputFoundOnly = Boolean.parseBoolean(props.getProperty(FOUND_TASK_ONLY, DEFAULT_FOUND_TASK_ONLY));
        allSrcFoundOnly = Boolean.parseBoolean(props.getProperty(ALL_SOURCE_FOUND, DEFAULT_ALL_SOURCE_FOUND));

        queueSetting = new HashMap<>();
        String queueString = props.getProperty(IMPALA_RESOURCE_POOL_LIST);
        if(queueString != null) {
            for (String pool : queueString.split(DEFAULT_LIST_DELIMITER)) {
                String[] poolSplit = pool.split(":");
                String poolName = poolSplit[0];
                double memLimit = Double.parseDouble(poolSplit[1]);
                queueSetting.put(poolName, memLimit);
            }
        }
    }

    /**
     * Get all queries from CM.
     * @return Map of Queries with target table as the key and QueryBase as value.
     * @throws Exception
     */
    public Map<String, QueryBase> getQueries() throws Exception {
        QuerySearchResult result = client.query(clusterName, serviceName, filter, from, to);

        int count = 0;
        ApiImpalaQuery query;
        while((query = result.nextQuery()) != null) {
            count += 1;

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Retriving record count=" + count);
            }

            TaskMetrics metrics = QueryAnalyzeUtil.collectMetricsFromQueryResponse(query);

            String statement = query.getStatement();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Query info: memory=" + metrics.getMaxMemoryGb() + "GB, duration=" + metrics.getDuration() + "s");
            }

            // If the SQL too long,
            if (statement.endsWith("...")) {
                LOGGER.info("Query too long for cm. Checking details for query " + query.getQueryId());
                try {
                    ApiImpalaQueryDetailsResponse detail = client.queryDetailThroughHTTP(clusterName, serviceName, query.getQueryId());
                    statement = QueryAnalyzeUtil.parseStatementFromDetail(detail);
                } catch (Exception e) {
                    LOGGER.error("Failed to get query details for id " + query.getQueryId(), e);
                    continue;
                }
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(statement);
            }

            try {
                QueryBase node = new QueryBase(statement, metrics);

                if(!node.getSource().isEmpty() && !node.getTarget().isEmpty()) {
                    if(LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Source Tables====");
                        for(String source : node.getSource()) {
                            LOGGER.debug(source);
                        }
                    }

                    if(LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Target Tables====");
                    }
                    for(String target : node.getTarget()) {
                        if(LOGGER.isDebugEnabled()) {
                            LOGGER.debug(target);
                        }
                        // Add queries to all target table.  Normally 1.
                        if(!allQueries.containsKey(target)) {
                            // We keep the latest SQL if duplicates found.
                            allQueries.put(target, node);
                        }
                    }
                }

            } catch (Exception e) {
                LOGGER.error("Failed to parse SQL: " + query.getStatement(), e);
                e.printStackTrace();
                continue;
            }
        }
        return allQueries;
    }

    /**
     * Get CM host.
     * @return CM host.
     */
    public String getHost() {
        return host;
    }

    /**
     * Should only output jobs with SQL found.
     * @return
     */
    public boolean isOutputFoundOnly() {
        return outputFoundOnly;
    }

    /**
     * Shouold only output jobs if all source tabls found.
     * @return
     */
    public boolean isAllSrcFoundOnly() {
        return allSrcFoundOnly;
    }

    /**
     * Get CM port.
     * @return Cm port.
     */
    public int getPort() {
        return port;
    }

    /**
     * Get API version.
     * @return
     */
    public String getVersion() {
        return version;
    }

    /**
     * If SSL enabled.
     * @return
     */
    public Boolean getSSLEnabled() {
        return isSSLEnabled;
    }

    /**
     * Get CA cert path.
     * @return CA cert path.
     */
    public String getPemPath() {
        return pemPath;
    }

    /**
     * Get cluster name.
     * @return Cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Get service name.
     * @return Service name.
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Get search start time.
     * @return Search start time.
     */
    public String getFrom() {
        return from;
    }

    /**
     * Get search end time.
     * @return Search end time.
     */
    public String getTo() {
        return to;
    }

    /**
     * Get search filter.
     * @return Search filter.
     */
    public String getFilter() {
        return filter;
    }

    /**
     * Set CM host name.
     * @param host CM host name.
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Set CM port.
     * @param port CM port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Set version.
     * @param version CM API version.
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Check SSL enabled.
     * @param SSLEnabled SSL enabled.
     */
    public void setSSLEnabled(Boolean SSLEnabled) {
        isSSLEnabled = SSLEnabled;
    }

    /**
     * Set ca cert path.
     * @param pemPath CA cert path.
     */
    public void setPemPath(String pemPath) {
        this.pemPath = pemPath;
    }

    /**
     * Set cluster name.
     * @param clusterName Cluster name.
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Set service name.
     * @param serviceName Service name.
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    /**
     * Set search start time.
     * @param from Search start time.
     */
    public void setFrom(String from) {
        this.from = from;
    }

    /**
     * Set search end time.
     * @param to Search end time.
     */
    public void setTo(String to) {
        this.to = to;
    }

    /**
     * Set search filter.
     * @param filter Search filter.
     */
    public void setFilter(String filter) {
        this.filter = filter;
    }

    /**
     * Get all found queries.
     * @return All found queries.
     */
    public Map<String, QueryBase> getAllQueries() {
        return allQueries;
    }

    /**
     * Set all queries.
     * @param allQueries All queries.
     */
    public void setAllQueries(Map<String, QueryBase> allQueries) {
        this.allQueries = allQueries;
    }

    /**
     * Get tables not to search.
     * @return Tables not search.
     */
    public Set<String> getExcludeTbls() {
        return excludeTbls;
    }

    /**
     * Get key words for tables not to search.
     * @return
     */
    public Set<String> getExcludeKeys() {
        return excludeKeys;
    }

    /**
     * Check if has more task.
     * @return True if more task.
     */
    public boolean hasNextTask() {
        return reader.hasNext();
    }

    /**
     * Get next task from input file and find all SQLs in CM response.
     * @return
     */
    public TaskInfoCollector nextTask() {
        String id = reader.next();
        LOGGER.info("Searching for query:" + id);
        TaskInfoCollector task = new TaskInfoCollector(id, reader.nextTargets(), reader.nextSources(), ignoreDB);
        task.findSqlWfs(getAllQueries(), getExcludeTbls(), getExcludeKeys());
        return task;
    }

    /**
     * Print header for csv output.
     * @return Header string.
     */
    public String prettyCsvHeader() {
        StringBuilder header = new StringBuilder();
        header.append("id,user,maxMemoryGB,TotalDuration,MaxDuration,Total Admission Wait,Total Input,Total Output" +
                ",File Formats,Pools,Found Source Tables,Not Found Source Tables,Total Query Count");
        if(!queueSetting.isEmpty()) {
            header.append(",Max Resource Pool,Pool Utility,Proper Pool");
        }
        return header.toString();
    }

    /**
     * The String is formmatted as id, user, maxMemoryGB, TotalDuration, MaxDuration, Total Admission Wait, TotalInput, Total Output
     * , File Formats,Pools,Found Source Tables, Not Found Source Tables, Total Query Count(, Max Resource Pool, Pool Utility, Proper Pool)
     *
     * @param task Collected task information to form the CSV.
     * @return CSV parsed String.
     */
    public String prettyCsvLine(TaskInfoCollector task) {

        StringBuilder output = new StringBuilder();
        output.append(task.toString());

        if(!queueSetting.isEmpty()) {
            Set<String> pools = task.getMetrics().getQueues();
            String largest = "";
            double maxQueueResource = 0;

            for(String pool : pools) {
                double resource = queueSetting.get(pool);
                if(resource > maxQueueResource) {
                    largest = pool;
                    maxQueueResource = resource;
                }
            }

            double utility = task.getMetrics().getMaxMemoryGb() / maxQueueResource * 100;

            String properPool = "Too large";
            double waste = Double.MAX_VALUE;

            for(Map.Entry<String, Double> pool : queueSetting.entrySet()) {
                if(pool.getValue() > task.getMetrics().getMaxMemoryGb()) {
                    double currentWaste = pool.getValue() - task.getMetrics().getMaxMemoryGb();
                    if (currentWaste < waste) {
                        waste = currentWaste;
                        properPool = pool.getKey();
                    }
                }
            }


            output.append(",").append(largest);
            output.append(",").append(utility);
            output.append(",").append(properPool);
        }


        return output.toString();
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 3) {
            LOGGER.error("Too few arguments");
            System.exit(1);
        }

        LOGGER.info("Starting to collect query information from CM.");

        Properties props = new Properties();
        props.load(new FileInputStream(args[0]));


        // Read all OM input and search for queries.
        QueryAnalyzer analyzer = new QueryAnalyzer(args[1], props);
        Map<String, QueryBase> allNodes = analyzer.getQueries();

//        TaskReader reader = TaskReaderFactory.getReader(args[1], props);
        BufferedWriter writer = new BufferedWriter(new FileWriter(args[2]));
//        String line;

        LOGGER.info("Finished collecting queryies. Trying to search jobs.");

        // Form the output.
        writer.write(analyzer.prettyCsvHeader());
        writer.newLine();
        while(analyzer.hasNextTask()) {
//            String[] split = line.split(DEFAULT_INPUT_SPLIT);
//            String id = split[0];
//            Set<String> targetTbls = new HashSet<>(Arrays.asList(split[1].split(DEFAULT_LIST_DELIMITER)));
//            Set<String> sourceTbls = new HashSet<>(Arrays.asList(split[2].split(DEFAULT_LIST_DELIMITER)));
//            String id = reader.next();
//            LOGGER.info("Searching for query:" + id);
//            SearchTask task = new SearchTask(id, reader.nextTargets(), reader.nextSources(), ignoreDB);
//            List<QueryBase> job = task.findSqlWfs(analyzer.getAllQueries(), analyzer.getExcludeTbls());
            TaskInfoCollector task = analyzer.nextTask();

//            String queryMostMem = "";
//            String queryLongest = "";
//
//            double maxMem = 0;
//            double maxDuration = 0;
//            double totalDuration = 0;
//            for (QueryBase query : job) {
//                if (LOGGER.isDebugEnabled()) {
//                    LOGGER.debug(query.getStatement());
//                }
//
//                if(query.getMemory() > maxMem) {
//                    maxMem = query.getMemory();
//                    queryMostMem = query.getStatement();
//                }
//
//                if(query.getDuration() > maxDuration) {
//                    maxDuration = query.getDuration();
//                    queryLongest = query.getStatement();
//                }
//
////                System.out.println(query.getStatement());
//                totalDuration += query.getDuration();
//            }
//            LOGGER.info("====ID : " + id + ", Mem : " + maxMem + "G, Duration : " + totalDuration);
//            writer.write(id + "," + maxMem + "," + totalDuration);
//            writer.newLine();
//            if (LOGGER.isDebugEnabled()) {
//                LOGGER.debug("Longest query: " + queryLongest);
//                LOGGER.debug("Query with largest memory: " + queryMostMem);
//            }

            // If skip empty result
            if(task.getQueryCount() == 0 && analyzer.isOutputFoundOnly()) {
                continue;
            } else if (!task.isAllSrcFound() && analyzer.isAllSrcFoundOnly()) {
                continue;
            }

            writer.write(analyzer.prettyCsvLine(task));
            writer.newLine();
        }
        writer.close();

    }
}
