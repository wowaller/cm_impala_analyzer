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

    public static final String JOB_ID = "omreader.job_id";
    public static final String MODELLING_STATE = "omreader.state";
    public static final String TARGET_DB = "omreader.target_db";
    public static final String TARGET_TABLE = "omreader.output_tbl";
    public static final String SOURCE_TABLE = "omreader.input_tbl";

    // Index of each column.
    public static final int DEFAULT_JOB_ID_INDEX = 3;
    public static final int DEFAULT_MODELLING_STATE_INDEX = 7;
    public static final int DEFAULT_TARGET_DB_NAME = 5;
    public static final int DEFAULT_TARGET_TABLE = 6;
    public static final int DEFAULT_SOURCE_TABLE = 15;

    private Map<String, Set<String>> srcTbls;
    private Map<String, Set<String>> tarTbls;
    private Iterator<String> keyItr;
    private String current;
    private int jobIdIndex;
    private int modellingStateIndex;
    private int targetDbIndex;
    private int targetTblIndex;
    private int sourceTblIndex;

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

        jobIdIndex = Integer.valueOf(props.getProperty(JOB_ID, String.valueOf(DEFAULT_JOB_ID_INDEX)));
        modellingStateIndex = Integer.valueOf(props.getProperty(MODELLING_STATE, String.valueOf(DEFAULT_MODELLING_STATE_INDEX)));
        targetDbIndex = Integer.valueOf(props.getProperty(TARGET_DB, String.valueOf(DEFAULT_TARGET_DB_NAME)));
        targetTblIndex = Integer.valueOf(props.getProperty(TARGET_TABLE, String.valueOf(DEFAULT_TARGET_TABLE)));
        sourceTblIndex = Integer.valueOf(props.getProperty(SOURCE_TABLE, String.valueOf(DEFAULT_SOURCE_TABLE)));

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

        if(split.length < DEFAULT_SOURCE_TABLE + 1) {
            LOGGER.error("Error parsing line " + line);
            return;
        }

        String id = split[jobIdIndex];
        LOGGER.info("Reading task: " + id);
        String targetDB = split[targetDbIndex];

        // Output table does not have DB information. So add DB name to the target table.
        // Remove all " in the original String
        String targetString = validateTarget(split[targetTblIndex]);
        String[] rawTarTbls = targetString.split(DEFAULT_EXCLUDE_TBL_LIST_DELIMITER);
        Set<String> targetTbls = new HashSet<>();
        for(String target : rawTarTbls) {
            targetTbls.add(targetDB + "." + target);
        }

        String sourceString = validateSource(split[sourceTblIndex]);
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

        if(split.length < DEFAULT_SOURCE_TABLE + 1) {
            LOGGER.error("Error parsing line " + line);
            return null;
        } else if(!split[modellingStateIndex].equalsIgnoreCase("model_success")){
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
