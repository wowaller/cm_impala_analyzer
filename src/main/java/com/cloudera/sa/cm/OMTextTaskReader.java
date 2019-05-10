package com.cloudera.sa.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Read csv saved from OM web.
 * Input format text file.
 */
public class OMTextTaskReader implements TaskReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(OMTextTaskReader.class);

    // Delimiter for target/spurce tables.
    public static final String DEFAULT_EXCLUDE_TBL_LIST_DELIMITER = ",";
    // Delimiter for each column.
    public static final String DEFAULT_INPUT_SPLIT = "\t";
    // If the input OM file has the header.
    public static final String SKIP_INPUT_HEADER = "skip_header";
    public static final String DEFAULT_SKIP_INPUT_HEADER = "false";

    // Index of each column.
    public static final int JOB_ID_INDEX = 3;
    public static final int MODELLING_STATE_INDEX = 7;
    public static final int TARGET_DB_NAME = 5;
    public static final int TARGET_TABLE = 6;
    public static final int SOURCE_TABLE = 15;

    private Map<String, Set<String>> srcTbls;
    private Map<String, Set<String>> tarTbls;
    private Iterator<String> keyItr;
    private String current;

    public OMTextTaskReader() {
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
     * Has more content.
     * @return True if has more.
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

        if(split.length < SOURCE_TABLE + 1) {
            LOGGER.error("Error parsing line " + line);
            return;
        }

        String id = split[JOB_ID_INDEX];
        LOGGER.info("Reading task: " + id);
        String targetDB = split[TARGET_DB_NAME];

        // Output table does not have DB information. So add DB name to the target table.
        // Remove all " in the original String
        String targetString = validateTarget(split[TARGET_TABLE]);
        String[] rawTarTbls = targetString.split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER);
        Set<String> targetTbls = new HashSet<>();
        for(String target : rawTarTbls) {
            targetTbls.add(targetDB + "." + target);
        }

        String sourceString = validateSource(split[SOURCE_TABLE]);
        Set<String> sourceTbls = new HashSet<>(Arrays.asList(sourceString.split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER)));

        // Same id could have a log lines in OM input. So for target/source tables group by the ID.
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

    /**
     * Check if line is valid. Not used in parse for now.
     * @param line Line of each input table.
     * @return Array of splitted columns.
     */
    private String[] validateParse(String line) {
        String[] split = line.split(DEFAULT_INPUT_SPLIT);

        if(split.length < SOURCE_TABLE + 1) {
            LOGGER.error("Error parsing line " + line);
            return null;
        } else if(!split[MODELLING_STATE_INDEX].equalsIgnoreCase("model_success")){
            LOGGER.error("Task failed in line " + line);
            return null;
        } else {
            return split;
        }
    }

    /**
     * Remove " from CSV input.
     * @param srcString List of raw source table column.
     * @return List of tables with " removed.
     */
    private String validateSource(String srcString) {
        return srcString.replace("\"", "");
    }

    /**
     * Remove " and (rowcount) from CSV input.
     * @param tarString List of raw target table column.
     * @return List of tables with " and (rowcount) removed.
     */
    private String validateTarget(String tarString) {
        String tmp = tarString.replace("\"", "");
        int countIndex = tmp.indexOf("(");
        return tmp.substring(0, countIndex);
    }
}
