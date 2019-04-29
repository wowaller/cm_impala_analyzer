package com.cloudera.sa.cm;

import com.cloudera.api.swagger.ImpalaQueriesResourceApi;
import com.cloudera.api.swagger.client.ApiClient;
import com.cloudera.api.swagger.client.ApiException;
import com.cloudera.api.swagger.model.ApiImpalaQuery;
import com.cloudera.api.swagger.model.ApiImpalaQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class QuerySearchResult {
    private static Logger LOGGER = LoggerFactory.getLogger(QuerySearchResult.class);

    private static final int QUERY_BATCH = 1000;
    private static final String QUERY_LIMIT_KEYWORD = "Impala query scan limit reached. Last end time considered is";

    private ImpalaQueriesResourceApi searchApi;
    private ApiImpalaQueryResponse currentResult;
    private Iterator<ApiImpalaQuery> currentItr;
    private String clusterName;
    private String serviceName;
    private String filter;
    private String from;
    private String nextEnd;
    private String to;
    private int offset = 0;

    public QuerySearchResult (ApiClient cmClient, String clusterName, String serviceName, String filter,
                              String from, String to) throws ApiException {
        this.searchApi = new ImpalaQueriesResourceApi(cmClient);
        this.clusterName = clusterName;
        this.serviceName = serviceName;
        this.filter = filter;
        this.from = from;
        this.nextEnd = to;
        this.to = to;

        this.offset = 0;

        doSearch();
    }

    public ApiImpalaQuery nextQuery() throws ApiException {
        if(currentItr.hasNext()) {
            return currentItr.next();
        } else {
            boolean hasMore = searchNext();
            if(hasMore && currentItr.hasNext()) {
                return currentItr.next();
            } else {
                return null;
            }
        }
    }

    public boolean hasNextQuery() throws ApiException {
        if (searchApi != null && currentResult != null && currentItr != null) {
            if (currentItr.hasNext()) {
                return true;
            } else {
                return searchNext();
            }
        }
        return false;
    }

    public boolean doSearch() throws ApiException {
        currentResult = searchApi.getImpalaQueries(clusterName, serviceName, filter, from, QUERY_BATCH, offset, nextEnd);
        if (currentResult.getQueries().size() != 0) {
            currentItr = currentResult.getQueries().iterator();
            return true;
        } else {
            return false;
        }
    }

    public boolean searchNextOffset() throws ApiException {
        offset += currentResult.getQueries().size();
        LOGGER.info("Start next query: " + offset + ", to=" + nextEnd);
        return doSearch();
    }

    public boolean searchNextTime() throws ApiException {
        nextEnd = from;
        for (String warning : currentResult.getWarnings()) {
            if (warning.contains(QUERY_LIMIT_KEYWORD)) {
                LOGGER.info(warning);
                nextEnd = warning.replace(QUERY_LIMIT_KEYWORD, "").trim();
            }
        }

        if (nextEnd.equals(from)) {
            return false;
        }

        offset = 0;
        LOGGER.info("Start next query: " + offset + ", to=" + nextEnd);
        return doSearch();
    }

    public boolean searchNext() throws ApiException {
        try {
            if (searchNextOffset()) {
                return true;
            } else if (searchNextTime()) {
                return true;
            } else {
                return false;
            }
        } catch (ApiException e) {
            LOGGER.error("Failed to get next search to CM.", e);
            return false;
        }
    }
}
