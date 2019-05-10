package com.cloudera.sa.cm;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * Interface of reader.
 */
public interface TaskReader {

    /**
     * Initialize reader on input path and read configuration from properties.
     * @param input Input file path.
     * @param props Configurations.
     * @throws IOException
     */
    public void initialize(String input, Properties props) throws IOException;

    /**
     * Has more content.
     * @return True if has more.
     */
    public boolean hasNext();

    /**
     * Has more content.
     * @return True if has more.
     */
    public String next();

    /**
     * Get source for current ID.
     * @return Sets of source tables.
     */
    public Set<String> nextSources();

    /**
     * Get target for current ID.
     * @return Sets of target tables.
     */
    public Set<String> nextTargets();
}
