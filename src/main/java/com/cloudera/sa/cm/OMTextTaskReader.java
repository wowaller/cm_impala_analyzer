package com.cloudera.sa.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class OMTextTaskReader implements TaskReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(OMTextTaskReader.class);

    public static final String DEFAULT_EXCLUDE_TBL_LIST_DELIMITER = ",";
    public static final String DEFAULT_INPUT_SPLIT = "\t";
    public static final String SKIP_INPUT_HEADER = "skip_header";
    public static final String DEFAULT_SKIP_INPUT_HEADER = "false";

    public static final int JOB_ID_INDEX = 3;
    public static final int TARGET_DB_NAME = 5;
    public static final int INPUT_TABLE = 6;
    public static final int SOURCE_TABLE = 15;

    private Map<String, Set<String>> srcTbls;
    private Map<String, Set<String>> tarTbls;
    private Iterator<String> keyItr;
    private String current;

    public OMTextTaskReader(String input, boolean skipHeader) {
        this.srcTbls = new HashMap<>();
        this.tarTbls = new HashMap<>();

    }

    public void initialize(String input, Properties props) throws IOException {

        BufferedReader reader = new BufferedReader(new FileReader(input));
        boolean skipHeader = Boolean.parseBoolean(props.getProperty(SKIP_INPUT_HEADER, DEFAULT_SKIP_INPUT_HEADER));

        String line;
        if(skipHeader) {
            reader.readLine();
        }
        while((line = reader.readLine()) != null) {
            parseLine(line);
        }
        keyItr = tarTbls.keySet().iterator();
        reader.close();
    }

    public boolean hasNext() {
        return keyItr.hasNext();
    }

    public String next() {
       current = keyItr.next();
       return current;
    }

    public Set<String> nextSources() {
        return srcTbls.get(current);
    }

    public Set<String> nextTargets() {
        return tarTbls.get(current);
    }

    private void parseLine(String line) {
        String[] split = line.split(DEFAULT_INPUT_SPLIT);

        if(split.length < SOURCE_TABLE + 1) {
            LOGGER.error("Error parsing line " + line);
            return;
        }

        String id = split[JOB_ID_INDEX];
        LOGGER.info("Reading task: " + id);
        String targetDB = split[TARGET_DB_NAME];

        // Output table does not have DB information. So add DB name to the target table.
        // Remove all " in the original String
        String targetString = validateTarget(split[INPUT_TABLE]);
        String[] rawTarTbls = targetString.split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER);
        Set<String> targetTbls = new HashSet<>();
        for(String target : rawTarTbls) {
            targetTbls.add(targetDB + "." + target);
        }

        String sourceString = validateSource(split[SOURCE_TABLE]);
        Set<String> sourceTbls = new HashSet<>(Arrays.asList(sourceString.split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER)));

        if(srcTbls.containsKey(id)) {
            srcTbls.get(id).addAll(sourceTbls);
        } else {
            srcTbls.put(id, sourceTbls);
        }

        if(tarTbls.containsKey(id)) {
            tarTbls.get(id).addAll(targetTbls);
        } else {
            tarTbls.put(id, targetTbls);
        }
    }

    private String validateSource(String srcString) {
        return srcString.replace("\"", "");
    }

    private String validateTarget(String tarString) {
        String tmp = tarString.replace("\"", "");
        int countIndex = tmp.indexOf("(");
        return tmp.substring(0, countIndex);
    }
}
