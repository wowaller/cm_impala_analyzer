package com.cloudera.sa.cm;

import com.cloudera.api.swagger.model.ApiImpalaQuery;
import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryAnalyzeUtil {
    // Attributes for one impala query.
    public static final String MEMORY_PER_NODE_PEAK = "memory_per_node_peak";
    public static final String ADMISSION_WAIT = "admission_wait";
    public static final String HDFS_BYTES_READ = "hdfs_bytes_read";
    public static final String HDFS_BYTES_WRITE = "hdfs_bytes_written";
    public static final String FILE_FORMATS = "file_formats";
    public static final String RESOURCE_POOL = "pool";
    public static final String USER = "user";

    /**
     * Collect statement from Impala detail response.
     * @param detail ApiImpalaQueryDetailsResponse from CM API.
     * @return String from detailed response.
     */
    public static String parseStatementFromDetail(ApiImpalaQueryDetailsResponse detail) {
        String impalaSqlExtractionPattern = ".*Sql Statement:(.*)Coordinator:.*";
        Pattern p = Pattern.compile(impalaSqlExtractionPattern);
        Matcher m = p.matcher(detail.getDetails().replaceAll("[\r\n]", " "));

        if(m.find()) {
            return m.group(1);
        } else {
            return null;
        }
    }

    /**
     * Get table name from full path.
     * @param path The db.tablename format.
     * @return Table name.
     */
    public static String getTblName(String path) {
        if(path.contains(".")) {
            return path.split("\\.")[1];
        } else {
            return path;
        }
    }

    /**
     * Is the all tables contain key words.
     * @param tbls The set tables.
     * @param excludeKey Keys to match.
     * @return True if the table has the key information.
     */
    public static boolean allExclude(Set<String> tbls, Set<String> excludeKey, Set<String> excludeTbls) {
        Set<String> tblsNotExclude = new HashSet<>(tbls);
        tblsNotExclude.removeAll(excludeTbls);
        for(String tbl : tblsNotExclude) {
            if(!hasKeyWd(tbl, excludeKey)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Is the table name contain key words.
     * @param tbl The table.
     * @param excludeKey Keys to match.
     * @return True if the table has the key information.
     */
    public static boolean hasKeyWd(String tbl, Set<String> excludeKey) {
        for(String key : excludeKey) {
            if(tbl.contains(key)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Create metrics from CM API response.
     * @param query ApiImpalaQuery from CM API.
     * @return Query metrics.
     */
    public static TaskMetrics collectMetricsFromQueryResponse(ApiImpalaQuery query) {
        String mem = query.getAttributes().get(MEMORY_PER_NODE_PEAK);
        BigDecimal durationMS = query.getDurationMillis();

        String admissionWaitString = query.getAttributes().get(ADMISSION_WAIT);
        double admissionWait = 0;
        if (admissionWaitString != null) {
            admissionWait = Double.parseDouble(admissionWaitString) / 1000;
        }

        double memGB = 0;
        double duration = 0;

        if (mem != null) {
            memGB = Double.parseDouble(mem) / 1024 / 1024 / 1024;
        }
        if (durationMS != null) {
            duration = durationMS.doubleValue() / 1000;
        }


        long inputBytes = 0;
        String inputBytesString = query.getAttributes().get(HDFS_BYTES_READ);
        if (inputBytesString != null) {
            inputBytes = Long.parseLong(inputBytesString);
        }
        long outputBytes = 0;
        String outputBytesString = query.getAttributes().get(HDFS_BYTES_WRITE);
        if (outputBytesString != null) {
            outputBytes = Long.parseLong(outputBytesString);
        }

        TaskMetrics metrics = new TaskMetrics();

        metrics.updateDuration(duration);
        metrics.updateMemoryGb(memGB);
        metrics.updateAdmissionWait(admissionWait);
        metrics.updateInputBytes(inputBytes);
        metrics.updateOutputBytes(outputBytes);

        String formatString = query.getAttributes().get(FILE_FORMATS);
        if (formatString != null) {
            for (String format : formatString.split(",")) {
                metrics.addInputFormat(format);
            }
        }

        String pool = query.getAttributes().get(RESOURCE_POOL);
        if (pool != null) {
            metrics.addQueue(pool);
        }

        metrics.addUser(query.getUser());

        return metrics;
    }
}
