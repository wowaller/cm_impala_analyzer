package com.cloudera.sa.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Perform information collection for one job.
 */
public class TaskInfoCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskInfoCollector.class);

    private String id;
    private Set<String> targetTbls;
    private Set<String> sourceTbls;
    private Set<String> foundSrcTbls;
    private Map<String, QueryBase> found;
    private Set<String> missed;
    private LinkedList<String> tableToScan;
    private boolean ignoreSrcDb;
    private TaskMetrics metrics;

    public TaskInfoCollector(String id, Set<String> targetTbls, Set<String> sourceTbls, boolean ignoreSrcDb) {
        this.id = id;
        this.targetTbls = targetTbls;
        this.sourceTbls = sourceTbls;
        this.found = new HashMap<>();
        this.missed = new HashSet<>();
        this.foundSrcTbls = new HashSet<>();
        this.tableToScan = new LinkedList<>();
        this.ignoreSrcDb = ignoreSrcDb;
        this.metrics = new TaskMetrics();
    }

    /**
     * Reset all collections in the class.
     */
    public void clear() {
        this.found = new HashMap<>();
        this.missed = new HashSet<>();
        this.foundSrcTbls = new HashSet<>();
        this.tableToScan = new LinkedList<>();
        this.metrics = new TaskMetrics();
    }

    /**
     * Use DFS algorithm to do the search.
     * For each target table, find SQL statements until source tables.
     * @param allQueries Set of all queries from CM search.
     * @param excludeTbls Tables ignore as source table.
     * @param excludeKey Keys if found in table should be ignored.
     * @return List of queries for the job.
     */
    public List<QueryBase> findSqlDfs(Map<String, QueryBase> allQueries, Set<String> excludeTbls, Set<String> excludeKey) {
        clear();
        for(String target : targetTbls) {
            if(allQueries.containsKey(target) && !excludeTbls.contains(target)
                    && !found.containsKey(target) && !inSrc(target) && !QueryAnalyzeUtil.hasKeyWd(target, excludeKey)) {
                // Only deal with found table and not in source / exclude / found list
                QueryBase current = allQueries.get(target);

                // If all the source tables has excludeKey then do nothing.
                if(!QueryAnalyzeUtil.allExclude(current.getSource(), excludeKey, excludeTbls)) {
                    found.put(target, current);
                    metrics.updateMetrics(current.getMetrics());
                    dfsTraverse(current, found, allQueries, excludeTbls, excludeKey);
                }
            } else if (inSrc(target)) {
                recordFoundSrc(target);
            } else if (!allQueries.containsKey(target) && !QueryAnalyzeUtil.hasKeyWd(target, excludeKey)
                    && !excludeTbls.contains(target)) {
                // Only record those not found and not in  exclude list
                missed.add(target);
            }
        }
        return new ArrayList<>(found.values());
    }

    /**
     * Inner function for DFS.
     * @param current Current query.
     * @param found Tables found in the search.
     * @param allQueries Set of all queries from CM search.
     * @param excludeTbls Tables ignore as source table.
     * @param excludeKey Keys if found in table should be ignored.
     */
    public void dfsTraverse(QueryBase current, Map<String, QueryBase> found,
                            Map<String, QueryBase> allQueries, Set<String> excludeTbls, Set<String> excludeKey) {
        for(String dependency : current.getSource()) {
            // The dependency is not seen yet and it's not the end of search.
            if(allQueries.containsKey(dependency) && !excludeTbls.contains(dependency)
                    && !found.containsKey(dependency) && !inSrc(dependency) && !QueryAnalyzeUtil.hasKeyWd(dependency, excludeKey)) {
                // Only deal with found table and not in source / exclude / found list
                QueryBase value = allQueries.get(dependency);

                // If all the source tables has excludeKey then do nothing.
                if(!QueryAnalyzeUtil.allExclude(value.getSource(), excludeKey, excludeTbls)) {
                    found.put(dependency, value);
                    metrics.updateMetrics(value.getMetrics());
                }
                dfsTraverse(value, found, allQueries, excludeTbls, excludeKey);
            } else if (inSrc(dependency)) {
                recordFoundSrc(dependency);
            } else if (!allQueries.containsKey(dependency) && !QueryAnalyzeUtil.hasKeyWd(dependency, excludeKey)
                    && !excludeTbls.contains(dependency)) {
                // Only record those not found and not in  exclude list
                missed.add(dependency);
            }
        }
    }

    /**
     * Use WFS algorithm to do the search.
     * For each target table, find SQL statements until source tables.
     * @param allQueries Set of all queries from CM search.
     * @param excludeTbls Tables ignore as source table.
     * @param excludeKey Keys if found in table should be ignored.
     * @return List of queries for the job.
     */
    public List<QueryBase> findSqlWfs(Map<String, QueryBase> allQueries, Set<String> excludeTbls, Set<String> excludeKey) {
//        Map<String, QueryBase> found = new HashMap<>();
//        LinkedList<String> tableToScan = new LinkedList<>();
        clear();
        tableToScan.addAll(targetTbls);

        while(!tableToScan.isEmpty()) {
            String current = tableToScan.pop();

            if(allQueries.containsKey(current) && !excludeTbls.contains(current)
                    && !found.containsKey(current) && !inSrc(current)  && !QueryAnalyzeUtil.hasKeyWd(current, excludeKey)) {
                // Only deal with found table and not in source / exclude / found list
                QueryBase value = allQueries.get(current);

                // If all the source tables has excludeKey then do nothing.
                if(!QueryAnalyzeUtil.allExclude(value.getSource(), excludeKey, excludeTbls)) {
                    found.put(current, value);
                    metrics.updateMetrics(value.getMetrics());
                    for (String dependency : value.getSource()) {
                        tableToScan.push(dependency);
                    }
                }
            } else if (inSrc(current)) {
                recordFoundSrc(current);
            } else if (!allQueries.containsKey(current) && !QueryAnalyzeUtil.hasKeyWd(current, excludeKey)
                    && !excludeTbls.contains(current)) {
                // Only record those not found and not in  exclude list
                missed.add(current);
            }
        }
        return new ArrayList<>(found.values());
    }

    /**
     * Is the table in source tablews.
     * @param tbl The table.
     * @return True if foun in source table.
     */
    public boolean inSrc(String tbl) {
        if(!ignoreSrcDb || !tbl.contains(".")) {
            // If dbname in the source set or there is no db name in source table.
            return sourceTbls.contains(tbl);
        } else {
            // If only tablename, then just get only tablename from input.
            String tblNoDb = tbl.split("\\.")[1];
            return sourceTbls.contains(tblNoDb);
        }
    }



    /**
     * Put all found source tables for further statistics.
     * @param tbl Found table.
     */
    public void recordFoundSrc(String tbl) {
        if(!ignoreSrcDb || !tbl.contains(".")) {
            foundSrcTbls.add(tbl);
        } else {
            String tblNoDb = tbl.split("\\.")[1];
            foundSrcTbls.add(tblNoDb);
        }
    }

    /**
     * Get metrics of the job.
     * @return Metrics of the job.
     */
    public TaskMetrics getMetrics() {
        return metrics;
    }

    /**
     * The String is formmatted as id, user, maxMemoryGB, TotalDuration, MaxDuration, Total Admission Wait, TotalInput, Total Output
     * , File Formats, Found Source Tables, Not Found Source Tables,Total Query Count.
     * @return Value string in csv format.
     */
    public String toString() {
        String queryMostMem = "";
        String queryLongest = "";

        if (LOGGER.isDebugEnabled()) {
            for (QueryBase query : found.values()) {
                LOGGER.debug(query.getStatement());

                if (query.getMetrics().getMaxMemoryGb() >= metrics.getMaxMemoryGb()) {
                    queryMostMem = query.getStatement();
                }

                if (query.getMetrics().getDuration() >= metrics.getMaxDuration()) {
                    queryLongest = query.getStatement();
                }

            }
        }
        LOGGER.info("====ID : " + id + ", Mem : " + metrics.getMaxMemoryGb() + "G, Duration : " + metrics.getDuration());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Longest query: " + queryLongest);
            LOGGER.debug("Query with largest memory: " + queryMostMem);
        }

        StringBuilder csvBuilder = new StringBuilder();
        csvBuilder.append(id).append(",");

        StringJoiner userSj = new StringJoiner("#");
        for(String user : metrics.getUsers()) {
            userSj.add(user);
        }
        csvBuilder.append(userSj.toString()).append(",");

        csvBuilder.append(metrics.getMaxMemoryGb()).append(",");
        csvBuilder.append(metrics.getDuration()).append(",");
        csvBuilder.append(metrics.getMaxDuration()).append(",");
        csvBuilder.append(metrics.getAdmissionDurtaion()).append(",");
        csvBuilder.append(metrics.getTotalInputBuytes()).append(",");
        csvBuilder.append(metrics.getTotalOutputBytes()).append(",");

        StringJoiner formatSj = new StringJoiner("#");
        for(String format : metrics.getFileFormats()) {
            formatSj.add(format);
        }
        csvBuilder.append(formatSj.toString()).append(",");

        StringJoiner queueSj = new StringJoiner("#");
        for(String queue : metrics.getQueues()) {
            queueSj.add(queue);
        }
        csvBuilder.append(queueSj.toString()).append(",");


        csvBuilder.append(getSrcFound().size()).append(",");
        csvBuilder.append(getMissedSrc().size()).append(",");
        csvBuilder.append(getQueryCount());

        return csvBuilder.toString();
    }

    /**
     * Get number of not found tables.
     * @return Number of not found tables.
     */
    public int notFoundTbls() {
        return missed.size();
    }

    /**
     * Get all queries found.
     * @return Set of queries found.
     */
    public int getQueryCount() {
        return found.values().size();
    }

    /**
     * Get job id.
     * @return Job id.
     */
    public String getId() {
        return id;
    }

    /**
     * Get queries found.
     * @return Queries found.
     */
    public Map<String, QueryBase> getFound() {
        return found;
    }

    /**
     * Get all missed statements.
     * @return Missed statements.
     */
    public Set<String> getMissed() {
        return missed;
    }

    /**
     * Get source tables not seen.
     * @return
     */
    public Set<String> getMissedSrc() {
        Set<String> ret = new HashSet<>(sourceTbls);
        ret.removeAll(foundSrcTbls);
        return ret;
    }

    /**
     * Get source tables seen.
     * @return
     */
    public Set<String> getSrcFound() {
        return foundSrcTbls;
    }


    /**
     * Check if all source tables found.
     * @return True if all source tables found.
     */
    public boolean isAllSrcFound() {
        return getMissedSrc().size() == 0;
    }
}
