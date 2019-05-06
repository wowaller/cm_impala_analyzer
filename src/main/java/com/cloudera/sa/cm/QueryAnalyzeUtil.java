package com.cloudera.sa.cm;

import com.cloudera.api.swagger.model.ApiImpalaQuery;
import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryAnalyzeUtil {
    public static final String MEMORY_PER_NODE_PEAK = "memory_per_node_peak";
    public static final String ADMISSION_WAIT = "admission_wait";
    public static final String HDFS_BYTES_READ = "hdfs_bytes_read";
    public static final String HDFS_BYTES_WRITE = "hdfs_bytes_written";
    public static final String FILE_FORMATS = "file_formats";
    public static final String RESOURCE_POOL = "pool";

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

    public static String getTblName(String path) {
        if(path.contains(".")) {
            return path.split("\\.")[1];
        } else {
            return path;
        }
    }

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

        return metrics;
    }
}
