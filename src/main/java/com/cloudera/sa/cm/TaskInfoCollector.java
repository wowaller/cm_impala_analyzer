package com.cloudera.sa.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TaskInfoCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskInfoCollector.class);

    private String id;
    private Set<String> targetTbls;
    private Set<String> sourceTbls;
    private Set<String> foundTargetTbls;
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
        this.foundTargetTbls = new HashSet<>();
        this.tableToScan = new LinkedList<>();
        this.ignoreSrcDb = ignoreSrcDb;
        this.metrics = new TaskMetrics();
    }

    public void clear() {
        this.found = new HashMap<>();
        this.missed = new HashSet<>();
        this.foundTargetTbls = new HashSet<>();
        this.tableToScan = new LinkedList<>();
        this.metrics = new TaskMetrics();
    }


    public List<QueryBase> findSqlDfs(Map<String, QueryBase> allQueries, Set<String> exclude) {
        clear();
        for(String target : targetTbls) {
            if(allQueries.containsKey(target) && !exclude.contains(target)
                    && !found.containsKey(target) && !inSrc(target)) {
                QueryBase current = allQueries.get(target);
                found.put(target, current);
                metrics.updateMetrics(current.getMetrics());
                dfsTraverse(current, found, allQueries, exclude);
            } else if (inSrc(target)) {
                foundTargetTbls.add(target);
            } else if (!allQueries.containsKey(target)) {
                missed.add(target);
            }
        }
        return new ArrayList<>(found.values());
    }

    public void dfsTraverse(QueryBase current, Map<String, QueryBase> found,
                            Map<String, QueryBase> allQueries, Set<String> exclude) {
        for(String dependency : current.getSource()) {
            // The dependency is not seen yet and it's not the end of search.
            if(allQueries.containsKey(dependency) && !exclude.contains(dependency)
                    && !found.containsKey(dependency) && !inSrc(dependency)) {
                QueryBase value = allQueries.get(dependency);
                found.put(dependency, value);
                metrics.updateMetrics(value.getMetrics());
                dfsTraverse(value, found, allQueries, exclude);
            } else if (inSrc(dependency)) {
                foundTargetTbls.add(dependency);
            } else if (!allQueries.containsKey(dependency)) {
                missed.add(dependency);
            }
        }
    }

    public List<QueryBase> findSqlWfs(Map<String, QueryBase> allQueries, Set<String> exclude) {
//        Map<String, QueryBase> found = new HashMap<>();
//        LinkedList<String> tableToScan = new LinkedList<>();
        clear();
        tableToScan.addAll(targetTbls);

        while(!tableToScan.isEmpty()) {
            String current = tableToScan.pop();

            if(allQueries.containsKey(current) && !exclude.contains(current)
                    && !found.containsKey(current) && !inSrc(current)) {
                QueryBase value = allQueries.get(current);
                found.put(current, value);
                metrics.updateMetrics(value.getMetrics());
                for(String dependency : value.getSource()) {
                    tableToScan.push(dependency);
                }
            } else if (inSrc(current)) {
                foundTargetTbls.add(current);
            } else if (!allQueries.containsKey(current)) {
                missed.add(current);
            }
        }
        return new ArrayList<>(found.values());
    }

    public boolean inSrc(String tbl) {
        if(!ignoreSrcDb || !tbl.contains(".")) {
            return sourceTbls.contains(tbl);
        } else {
            String tblNoDb = tbl.split("\\.")[1];
            return sourceTbls.contains(tblNoDb);
        }
    }

    public TaskMetrics getMetrics() {
        return metrics;
    }

    /**
     * The String is formmatted as id, user, maxMemoryGB, TotalDuration, MaxDuration, Total Admission Wait, TotalInput, Total Output
     * , File Formats, Not Found SQL Number, Total Query Count.

     * @return
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

        StringJoiner userSj = new StringJoiner("|");
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

        StringJoiner formatSj = new StringJoiner("|");
        for(String format : metrics.getFileFormats()) {
            formatSj.add(format);
        }
        csvBuilder.append(formatSj.toString()).append(",");

        StringJoiner queueSj = new StringJoiner("|");
        for(String queue : metrics.getQueues()) {
            queueSj.add(queue);
        }
        csvBuilder.append(queueSj.toString()).append(",");


        csvBuilder.append(notFoundTbls()).append(",");
        csvBuilder.append(getQueryCount());

        return csvBuilder.toString();
    }

    public int notFoundTbls() {
        return missed.size();
    }

    public int getQueryCount() {
        return found.values().size();
    }

    public String getId() {
        return id;
    }

    public Map<String, QueryBase> getFound() {
        return found;
    }

    public Set<String> getMissed() {
        return missed;
    }

    public boolean isAllSrcFound() {
        return foundTargetTbls.containsAll(targetTbls);
    }
}
