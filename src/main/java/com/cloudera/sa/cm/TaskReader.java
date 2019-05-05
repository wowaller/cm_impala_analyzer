package com.cloudera.sa.cm;

import java.io.IOException;
import java.util.Properties;
import java.util.Set;

public interface TaskReader {

    public void initialize(String input, Properties props) throws IOException;

    public boolean hasNext();

    public String next();

    public Set<String> nextSources();

    public Set<String> nextTargets();
}
