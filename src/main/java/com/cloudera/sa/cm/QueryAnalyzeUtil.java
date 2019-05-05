package com.cloudera.sa.cm;

import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryAnalyzeUtil {

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
}
