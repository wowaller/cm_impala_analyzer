package com.cloudera.sa.cm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class SearchTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(SearchTask.class);

    private String id;
    private Set<String> targetTbls;
    private Set<String> sourceTbls;
    private Map<String, QueryBase> found;
    private LinkedList<String> tableToScan;

    public SearchTask(String id, Set<String> targetTbls, Set<String> sourceTbls) {
        this.id = id;
        this.targetTbls = targetTbls;
        this.sourceTbls = sourceTbls;
        this.found = new HashMap<>();
        this.tableToScan = new LinkedList<>();
    }


    public List<QueryBase> findSqlDfs(Map<String, QueryBase> allQueries, Set<String> exclude) {
        for(String target : targetTbls) {
            QueryBase current = allQueries.get(target);
            if(current != null && !exclude.contains(current)
                    && !found.containsKey(current) && !sourceTbls.contains(current)) {
                found.put(target, current);
                dfsTraverse(current, sourceTbls, found, allQueries, exclude);
            }
        }
        return new ArrayList<>(found.values());
    }

    public void dfsTraverse(QueryBase current, Set<String> stopTbls, Map<String, QueryBase> found,
                            Map<String, QueryBase> allQueries, Set<String> exclude) {
        for(String dependency : current.getSource()) {
            // The dependency is not seen yet and it's not the end of search.
            if(allQueries.containsKey(dependency) && !exclude.contains(dependency)
                    && !found.containsKey(dependency) && !stopTbls.contains(dependency)) {
                QueryBase value = allQueries.get(dependency);
                found.put(dependency, value);
                dfsTraverse(value, stopTbls, found, allQueries, exclude);
            }
        }
    }

    public List<QueryBase> findSqlWfs(Map<String, QueryBase> allQueries, Set<String> exclude) {
//        Map<String, QueryBase> found = new HashMap<>();
//        LinkedList<String> tableToScan = new LinkedList<>();
        tableToScan.addAll(targetTbls);

        while(!tableToScan.isEmpty()) {
            String current = tableToScan.pop();

            if(allQueries.containsKey(current) && !exclude.contains(current)
                    && !found.containsKey(current) && !sourceTbls.contains(current)) {
                QueryBase value = allQueries.get(current);
                found.put(current, value);
                for(String dependency : value.getSource()) {
                    tableToScan.push(dependency);
                }
            }
        }
        return new ArrayList<>(found.values());
    }

    public String getCSVResult() {

        String queryMostMem = "";
        String queryLongest = "";

        double maxMem = 0;
        double maxDuration = 0;
        double totalDuration = 0;
        for (QueryBase query : found.values()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(query.getStatement());
            }

            if(query.getMemory() > maxMem) {
                maxMem = query.getMemory();
                queryMostMem = query.getStatement();
            }

            if(query.getDuration() > maxDuration) {
                maxDuration = query.getDuration();
                queryLongest = query.getStatement();
            }

//                System.out.println(query.getStatement());
            totalDuration += query.getDuration();
        }
        LOGGER.info("====ID : " + id + ", Mem : " + maxMem + "G, Duration : " + totalDuration);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Longest query: " + queryLongest);
            LOGGER.debug("Query with largest memory: " + queryMostMem);
        }
        return id + "," + maxMem + "G," + totalDuration;
    }
}
