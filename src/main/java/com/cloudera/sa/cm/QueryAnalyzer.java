package com.cloudera.sa.cm;

import com.cloudera.api.swagger.model.ApiImpalaQuery;
import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.math.BigDecimal;
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
    public static final String DEFAULT_EXCLUDE_TBL_LIST_DELIMITER = ",";

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

    private Map<String, QueryBase> allQueries;
    private Set<String> exclude;

    public QueryAnalyzer(Properties props) {
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
            exclude.addAll(Arrays.asList(excludeString.split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER)));
        }
    }

    public Map<String, QueryBase> getQueries() throws Exception {
        QuerySearchResult result = client.query(clusterName, serviceName, filter, from, to);

        int count = 0;
        ApiImpalaQuery query;
        while((query = result.nextQuery()) != null) {
            count += 1;
            LOGGER.info("Retriving record count=" + count);

            String mem = query.getAttributes().get("memory_per_node_peak");
            BigDecimal durationMS = query.getDurationMillis();
            String adminWait = query.getAttributes().get("admission_wait");

            double memGB = 0;
            double duration = 0;

            if (mem != null) {
                memGB = Double.parseDouble(mem) / 1024 / 1024 / 1024;
            }
            if (durationMS != null) {
                duration = durationMS.doubleValue() / 1000;
            }
            String statement = query.getStatement();

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Query info: memory=" + memGB + "GB, duration=" + duration + "s");
            }

            if (statement.endsWith("...")) {
                LOGGER.info("Query too long for cm. Checking details for query " + query.getQueryId());
                try {
                    ApiImpalaQueryDetailsResponse detail = client.queryDetailThroughHTTP(clusterName, serviceName, query.getQueryId());
                    statement = QueryBase.parseStatementFromDetail(detail);
                } catch (Exception e) {
                    LOGGER.error("Failed to get query details for id " + query.getQueryId(), e);
                    continue;
                }
            }

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(statement);
            }

            try {
                QueryBase node = new QueryBase(statement, duration, memGB);

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
                        allQueries.put(target, node);
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

    public List<QueryBase> findSqlDfs(Set<String> targetTbls, Set<String> sourceTbls) {
        Map<String, QueryBase> sqls = new HashMap<>();

        for(String target : targetTbls) {
            QueryBase current = allQueries.get(target);
            if(current != null) {
                sqls.put(target, current);
                dfsTraverse(current, sourceTbls, sqls);
            }
        }
        return new ArrayList<QueryBase>(sqls.values());
    }

    public void dfsTraverse(QueryBase current, Set<String> stopTbls, Map<String, QueryBase> found) {
        for(String dependency : current.getSource()) {
            // The dependency is not seen yet and it's not the end of search.
            if(allQueries.containsKey(dependency) && !exclude.contains(dependency)
                    && !found.containsKey(dependency) && !stopTbls.contains(dependency)) {
                QueryBase value = allQueries.get(dependency);
                found.put(dependency, value);
                dfsTraverse(value, stopTbls, found);
            }
        }
    }

    public List<QueryBase> findSqlWfs(Set<String> targetTbls, Set<String> sourceTbls) {
        Map<String, QueryBase> found = new HashMap<>();
        LinkedList<String> tableToScan = new LinkedList<>();
        tableToScan.addAll(targetTbls);

        while(!tableToScan.isEmpty()) {
            String current = tableToScan.pop();

            if(allQueries.containsKey(current) && !exclude.contains(current)
                    && !found.containsKey(current) && !sourceTbls.contains(current)) {
                QueryBase value = allQueries.get(current);
                found.put(current, value);
                for(String dependency : value.getSource()) {
                    tableToScan.push(dependency);
                }
            }
        }
        return new ArrayList<QueryBase>(found.values());
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

    public static void main(String[] args) throws Exception {
        if(args.length < 2) {
            LOGGER.error("Too few arguments");
            System.exit(1);
        }

        Properties props = new Properties();
        props.load(new FileInputStream(args[0]));

        BufferedReader reader = new BufferedReader(new FileReader(args[1]));

        QueryAnalyzer analyzer = new QueryAnalyzer(props);
        Map<String, QueryBase> allNodes = analyzer.getQueries();

        String line;

        while((line = reader.readLine()) != null) {
            String[] split = line.split(DEFAULT_INPUT_SPLIT);
            String id = split[0];
            Set<String> targetTbls = new HashSet<>(Arrays.asList(split[1].split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER)));
            Set<String> sourceTbls = new HashSet<>(Arrays.asList(split[2].split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER)));
            List<QueryBase> job = analyzer.findSqlWfs(targetTbls, sourceTbls);

            double maxMem = 0;
            double totalDuration = 0;
            for (QueryBase query : job) {
                System.out.println(query.getStatement());
                maxMem = Math.max(maxMem, query.getMemory());
                totalDuration += query.getDuration();
            }
            System.out.println("====ID : " + id + ", Mem : " + maxMem + "G, Duration : " + totalDuration);
        }
    }
}
