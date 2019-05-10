package com.cloudera.sa.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Default reader for Job information.
 * Input format text file.
 * Format: ID \t target_table1,target_table2,... \t source_table1,source_table2,...
 */
public class DefaultTaskReader implements TaskReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultTaskReader.class);

    // Delimiter for target/source table list.
    public static final String DEFAULT_EXCLUDE_TBL_LIST_DELIMITER = ",";
    // Delimiter for id / target tables / source tables.
    public static final String DEFAULT_INPUT_SPLIT = "\t";
    // If input has header.
    public static final String SKIP_INPUT_HEADER = "skip_header";
    public static final String DEFAULT_SKIP_INPUT_HEADER = "false";

    private Map<String, Set<String>> srcTbls;
    private Map<String, Set<String>> tarTbls;
    private Iterator<String> keyItr;
    private String current;

    public DefaultTaskReader() {
        this.srcTbls = new HashMap<>();
        this.tarTbls = new HashMap<>();

    }

    /**
     * Initialize reader on input path and read configuration from properties.
     * @param input Input file path.
     * @param props Configurations.
     * @throws IOException
     */
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

    /**
     * Has more content.
     * @return True if has more.
     */
    public boolean hasNext() {
        return keyItr.hasNext();
    }

    /**
     * Get next ID of input.
     * @return Next ID of input.
     */
    public String next() {
       current = keyItr.next();
       return current;
    }

    /**
     * Get source for current ID.
     * @return Sets of source tables.
     */
    public Set<String> nextSources() {
        return srcTbls.get(current);
    }

    /**
     * Get target for current ID.
     * @return Sets of target tables.
     */
    public Set<String> nextTargets() {
        return tarTbls.get(current);
    }

    /**
     * Parse line in the input file
     * @param line Line read from input file.
     */
    private void parseLine(String line) {
        String[] split = line.split(DEFAULT_INPUT_SPLIT);
        String id = split[0];
        LOGGER.info("Reading task: " + id);
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
