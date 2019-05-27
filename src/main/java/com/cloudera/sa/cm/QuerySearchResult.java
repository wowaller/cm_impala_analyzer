package com.cloudera.sa.cm;

import com.cloudera.api.swagger.ImpalaQueriesResourceApi;
import com.cloudera.api.swagger.client.ApiClient;
import com.cloudera.api.swagger.client.ApiException;
import com.cloudera.api.swagger.model.ApiImpalaQuery;
import com.cloudera.api.swagger.model.ApiImpalaQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Get all search result from CM.
 */
public class QuerySearchResult {
    private static Logger LOGGER = LoggerFactory.getLogger(QuerySearchResult.class);

    private static final int QUERY_BATCH = 1000;
    // Used to check if there's more from CM.
    // It means CM has more queries and please use the time in "Last end time" for to=xxx in next search.
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
    private int offset;

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

    /**
     * Get next query from CM API.
     * @return ApiImpalaQuery from CM API as next query found.
     * @throws ApiException
     */
    public ApiImpalaQuery nextQuery() throws ApiException {
        if(currentItr == null) {
            searchNextTime();
        }
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

    /**
     * Check if there's more result from CM.
     * @return True if has more query.
     * @throws ApiException
     */
    public boolean hasNextQuery() throws ApiException {
        if (searchApi != null && currentResult != null && currentItr != null) {
            if (currentItr.hasNext()) {
                // If we have more queries in current result.
                return true;
            } else {
                // If more results to search from CM.
                return searchNext();
            }
        }
        return false;
    }

    /**
     * Perform on search to CM.
     * @return True if results found.
     * @throws ApiException
     */
    public boolean doSearch() throws ApiException {
        currentResult = searchApi.getImpalaQueries(clusterName, serviceName, filter, from, QUERY_BATCH, offset, nextEnd);
        if (currentResult.getQueries().size() != 0) {
            currentItr = currentResult.getQueries().iterator();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Perform a search with updated offset to CM.
     * @return True if more found.
     * @throws ApiException
     */
    public boolean searchNextOffset() throws ApiException {
        offset += currentResult.getQueries().size();
        LOGGER.info("Start next query: " + offset + ", to=" + nextEnd);
        return doSearch();
    }

    /**
     * Perform a search with updated to time.
     * For CM API, one time range could only return certain number of rows.
     * Need to check warning information to get the actual time range used as next search.
     * @return True if more found.
     * @throws ApiException
     */
    public boolean searchNextTime() throws ApiException {
        nextEnd = from;
        // Check the new "to" (end time).
        for (String warning : currentResult.getWarnings()) {
            if (warning.contains(QUERY_LIMIT_KEYWORD)) {
                LOGGER.info(warning);
                nextEnd = warning.replace(QUERY_LIMIT_KEYWORD, "").trim();
            }
        }

        // If from=to
        if (nextEnd.equals(from)) {
            return false;
        }

        // Reset the offset as we are starting a new search.
        offset = 0;
        LOGGER.info("Start next query: " + offset + ", to=" + nextEnd);
        boolean hasNext = doSearch();
        if(hasNext) {
            return true;
        } else {
            return searchNextTime();
        }
    }

    /**
     * The overall search function.
     * Do updated offset search first.
     * If none found, do updated to search.
     * @return True if more result found.
     * @throws ApiException
     */
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
