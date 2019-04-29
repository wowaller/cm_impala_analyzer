package com.cloudera.sa.cm;

public class AnalyzerConf {

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

}
