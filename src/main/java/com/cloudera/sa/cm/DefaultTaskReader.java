package com.cloudera.sa.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DefaultTaskReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTaskReader.class);

    public static final String DEFAULT_EXCLUDE_TBL_LIST_DELIMITER = ",";
    public static final String DEFAULT_INPUT_SPLIT = "\t";

    private String input;
    private Map<String, Set<String>> srcTbls;
    private Map<String, Set<String>> tarTbls;
    private Iterator<String> keyItr;

    public DefaultTaskReader(String input) throws IOException {
        this.input = input;
        this.srcTbls = new HashMap<>();
        this.tarTbls = new HashMap<>();

        BufferedReader reader = new BufferedReader(new FileReader(input));

        String line;
        while((line = reader.readLine()) != null) {
            parseLine(line);
        }
        initialize();
    }

    public void initialize() {
        keyItr = tarTbls.keySet().iterator();
    }

    public boolean hasNext() {
        return keyItr.hasNext();
    }

    public SearchTask nextTask() {
        String id = keyItr.next();
        return new SearchTask(id, tarTbls.get(id), srcTbls.get(id));
    }

    private void parseLine(String line) {
        String[] split = line.split(DEFAULT_INPUT_SPLIT);
        String id = split[0];
        LOGGER.info("Searching for query:" + id);
        Set<String> targetTbls = new HashSet<>(Arrays.asList(split[1].split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER)));
        Set<String> sourceTbls = new HashSet<>(Arrays.asList(split[2].split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER)));
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
}
