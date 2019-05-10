package com.cloudera.sa.cm;

import com.cloudera.api.swagger.ImpalaQueriesResourceApi;
import com.cloudera.api.swagger.client.ApiClient;
import com.cloudera.api.swagger.client.ApiException;
import com.cloudera.api.swagger.client.Configuration;
import com.cloudera.api.swagger.client.JSON;
import com.cloudera.api.swagger.model.ApiImpalaCancelResponse;
import com.cloudera.api.swagger.model.ApiImpalaQueryDetailsResponse;
import com.cloudera.api.swagger.model.ApiImpalaQueryResponse;
import com.google.common.reflect.TypeToken;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

/**
 * Class to get Impala queries from CM.
 */
public class ImpalaQuerySearch {
    private static final Logger LOGGER = LoggerFactory.getLogger(ImpalaQuerySearch.class);

    private ApiClient cmClient;
    private String host;
    private int port;
    private String version;
    private String username;
    private String password;

    private boolean isSSLEnabled;
    private String pemPath;

    public ImpalaQuerySearch(String host, Integer port, String version, String username, String password,
                             Boolean isSSLEnabled, String pemPath) {
        this.host = host;
        this.port = port;
        this.version = version;
        this.password = password;
        this.username = username;

        this.isSSLEnabled = isSSLEnabled;
        this.pemPath = pemPath;

        cmClient = getCMClient(host, port, version, username, password, isSSLEnabled, pemPath);
    }

    public ApiClient getCMClient(String host, Integer port, String version, String username, String password) {
        return getCMClient(host, port, version, username, password, false, "");
    }

    /**
     * Construct URL string for basic CM API.
     * @param host CM host.
     * @param port CM port.
     * @param version CM API version, for example, v19 for cm5.15
     * @param isSSLEnabled Is ssl enabled on CM.
     * @return HTTP URL for cm rest API.
     */
    public String getBaseUrl(String host, Integer port, String version, Boolean isSSLEnabled) {
        String protocol = "http";
        if (isSSLEnabled) {
            protocol = "https";
        }
        return protocol + "://" + host + ":" + port + "/api/" + version;
    }

    /**
     * Construct ApiClient as the client class in CM JAVA API.
     * @param host CM host.
     * @param port CM port.
     * @param version CM API version, for example, v19 for cm5.15.
     * @param username User name to log in to CM.
     * @param password Password to log in to CM.
     * @param isSSLEnabled Is ssl enabled on CM.
     * @param pemPath The ca cert for SSL.
     * @return ApiClient for CM Java API.
     */
    public ApiClient getCMClient(String host, Integer port, String version, String username, String password,
                                Boolean isSSLEnabled, String pemPath) {
        ApiClient cmClient = Configuration.getDefaultApiClient();

        cmClient.setBasePath(getBaseUrl(host, port, version, isSSLEnabled));
        cmClient.setUsername(username);
        cmClient.setPassword(password);

        if (isSSLEnabled) {
            try {
                cmClient.setVerifyingSsl(true);
                Path truststorePath = Paths.get(pemPath);
                byte[] truststoreBytes = Files.readAllBytes(truststorePath);
                cmClient.setSslCaCert(new ByteArrayInputStream(truststoreBytes));
            } catch (IOException e) {
                LOGGER.error("Exception when reading ssl pem", e);
                e.printStackTrace();
            }
        }
        return cmClient;
    }

    /**
     * Directly get query response from CM API.
     * @param clusterName Cluster name. Usually cluster.
     * @param serviceName Impala Service name. Usually impala.
     * @param filter The filter for Impala query.
     * @param from ISO8601 format time for the start time of query.
     * @param to ISO8601 format time for the end time of query.
     * @param limit Result limit. Max value is 1000.
     * @param offset Start of next query. Since max limit is 1000, we will need several queries with incremental offset.
     * @return Results of each query.
     * @throws ApiException
     */
    public ApiImpalaQueryResponse queryRaw(String clusterName, String serviceName, String filter, String from,
                                            String to, int limit, int offset) throws ApiException {
        ApiImpalaQueryResponse result;

        ImpalaQueriesResourceApi apiInstance = new ImpalaQueriesResourceApi(cmClient);
        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug(clusterName + "," + serviceName + "," + filter + "," + from + "," + limit + "," + offset + "," + to);
        }

        result = apiInstance.getImpalaQueries(clusterName, serviceName, filter, from, limit, offset, to);

        return result;
    }

    /**
     * Get detailed query information. Useful if SQL is too long.
     * @param clusterName Cluster name. Usually cluster.
     * @param serviceName Service name. Usually impala.
     * @param queryId The ID for detailed Impala query from CM API.
     * @return Impala detailed query response.
     * @throws ApiException
     */
    public ApiImpalaQueryDetailsResponse queryDetailRaw(String clusterName, String serviceName, String queryId) throws ApiException {
        ApiImpalaQueryDetailsResponse result;

        ImpalaQueriesResourceApi apiInstance = new ImpalaQueriesResourceApi(cmClient);

        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug(clusterName + "," + serviceName + "," + queryId);
        }
        result = apiInstance.getQueryDetails(clusterName, queryId, serviceName, "text");

        return result;
    }

    /**
     * Use HTTP request instead of JAVA API to get detailed information. It is useful as CM JAVA has some problem
     * with query detailed request. After that, the response string is packaged as a detialed query response.
     * @param clusterName Cluster name. Usually cluster.
     * @param serviceName Service name. Usually impala.
     * @param queryId The ID for detailed Impala query from CM API.
     * @return Impala detailed query response.
     */
    public ApiImpalaQueryDetailsResponse queryDetailThroughHTTP(String clusterName, String serviceName, String queryId) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getUrlEncoder().encode(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);

        CloseableHttpClient client = HttpClients.createDefault();

        String baseUrl = getBaseUrl(host, port, version, false);

        HttpGet get = new HttpGet(baseUrl + "/clusters/" + clusterName + "/services/" + serviceName + "/impalaQueries/" + queryId);
        get.addHeader("Authorization", authHeader);

        String responseContent = null;
        CloseableHttpResponse response = null;
        try {
            response = client.execute(get);
            if(response.getStatusLine().getStatusCode() == 200) {
                HttpEntity entity = response.getEntity();
                responseContent = EntityUtils.toString(entity, "UTF-8");
            }

            if (response != null) {
                response.close();
            }
            if (client != null) {
                client.close();
            }

            JSON json = new JSON(cmClient);
            Type returnType = new TypeToken<ApiImpalaQueryDetailsResponse>(){}.getType();

            return (ApiImpalaQueryDetailsResponse) json.deserialize(responseContent, returnType);
        } catch (IOException e) {
            LOGGER.error("Error in getting detailed query", e);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Query list of Impala queries. Note that the queries can be too long to store the whole query.
     * Use queryDetailThroughHTTP to get the whole SQL with ID returned.
     * @param clusterName Cluster name. Usually cluster.
     * @param serviceName Service name. Usually impala.
     * @param filter The filter for Impala query.
     * @param from ISO8601 format time for the start time of query.
     * @param to ISO8601 format time for the end time of query.
     * @return QuerySearchResult to iterate query result.
     * @throws ApiException
     */
    public QuerySearchResult query(String clusterName, String serviceName, String filter, String from, String to) throws ApiException {
        return new QuerySearchResult(cmClient, clusterName, serviceName, filter, from, to);
    }

}
