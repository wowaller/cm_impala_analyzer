package com.cloudera.sa.cm;

import com.cloudera.api.swagger.model.ApiImpalaQuery;
import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class QueryAnalyzer {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryAnalyzer.class);

    public static final String CM_HOST = "cm_host";
    public static final String CM_PORT = "cm_port";
    public static final String CLUSTER_NAME = "cluster_name";
    public static final String SERVICE_NAME = "service_name";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String API_VERSION = "api_version";

    public static final String ENABLE_SSL = "enable_ssl";
    public static final String DEFAULT_ENABLE_SSL = "false";
    public static final String PEM_PATH = "ssl_pem_path";
    public static final String DEFAULT_PEM_PATH = "";

    public static final String QUERY_START_TIME = "start_time";
    public static final String QUERY_END_TIME = "end_time";
    public static final String QUERY_FILTER = "filter";
    public static final String DEFAULT_QUERY_FILTER = "";
    public static final String EXCLUDE_TBL_LIST = "excludeTbls";
    public static final String DEFAULT_LIST_DELIMITER = ",";
    public static final String IGNORE_DB_NAME = "ignore_db";
    public static final String DEFAULT_IGNORE_DB_NAME = "false";
    public static final String IMPALA_RESOURCE_POOL_LIST = "resource_pool";

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

    private Map<String, QueryBase> allQueries;

    private Set<String> exclude;

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
        exclude = new HashSet<>();
        if(excludeString != null) {
            exclude.addAll(Arrays.asList(excludeString.split(DEFAULT_LIST_DELIMITER)));
        }

        reader = TaskReaderFactory.getReader(input, props);
        ignoreDB = Boolean.parseBoolean(props.getProperty(IGNORE_DB_NAME, DEFAULT_IGNORE_DB_NAME));

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

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getVersion() {
        return version;
    }

    public Boolean getSSLEnabled() {
        return isSSLEnabled;
    }

    public String getPemPath() {
        return pemPath;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public String getFilter() {
        return filter;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void setSSLEnabled(Boolean SSLEnabled) {
        isSSLEnabled = SSLEnabled;
    }

    public void setPemPath(String pemPath) {
        this.pemPath = pemPath;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Map<String, QueryBase> getAllQueries() {
        return allQueries;
    }

    public void setAllQueries(Map<String, QueryBase> allQueries) {
        this.allQueries = allQueries;
    }

    public Set<String> getExclude() {
        return exclude;
    }

    public boolean hasNextTask() {
        return reader.hasNext();
    }

    public TaskInfoCollector nextTask() {
        String id = reader.next();
        LOGGER.info("Searching for query:" + id);
        TaskInfoCollector task = new TaskInfoCollector(id, reader.nextTargets(), reader.nextSources(), ignoreDB);
        task.findSqlWfs(getAllQueries(), getExclude());
        return task;
    }


    public String prettyCsvHeader() {
        StringBuilder header = new StringBuilder();
        header.append("id,user,maxMemoryGB,TotalDuration,MaxDuration,Total Admission Wait,TotalInput,Total Output" +
                ",File Formats,Not Found SQL Number,Total Query Count");
        if(!queueSetting.isEmpty()) {
            header.append(",Max Resource Pool,Pool Utility,Proper Pool");
        }
        return header.toString();
    }

    /**
     * The String is formmatted as id, user, maxMemoryGB, TotalDuration, MaxDuration, Total Admission Wait, TotalInput, Total Output
     * , File Formats, Not Found SQL Number, Total Query Count(, Max Resource Pool, Pool Utility, Proper Pool)
     *
     * @param task
     * @return
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

            double utility = task.getMetrics().getMaxMemoryGb() / maxQueueResource;

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


        QueryAnalyzer analyzer = new QueryAnalyzer(args[1], props);
        Map<String, QueryBase> allNodes = analyzer.getQueries();

//        TaskReader reader = TaskReaderFactory.getReader(args[1], props);
        BufferedWriter writer = new BufferedWriter(new FileWriter(args[2]));
//        String line;

        LOGGER.info("Finished collecting queryies. Trying to search jobs.");

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
//            List<QueryBase> job = task.findSqlWfs(analyzer.getAllQueries(), analyzer.getExclude());
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
            writer.write(analyzer.prettyCsvLine(task));
            writer.newLine();
        }
        writer.close();

    }
}
