package com.cloudera.sa.cm;

import java.io.IOException;
import java.util.Properties;

public class TaskReaderFactory {

    public static final String READER_CLASS = "reader_class";
    public static final String DEFAULT_READER_CLASS = "com.cloudera.sa.cm.DefaultTaskReader";

    public static TaskReader getReader(String input, Properties props) throws ClassNotFoundException,
            IllegalAccessException, InstantiationException, IOException {
        TaskReader reader = (TaskReader) Class.forName(props.getProperty(READER_CLASS, DEFAULT_READER_CLASS)).newInstance();
        reader.initialize(input, props);
        return reader;
    }
}
