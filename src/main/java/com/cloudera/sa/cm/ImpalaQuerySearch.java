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

        cmClient = getCMClient(host, port, version, username, password, isSSLEnabled, pemPath);
    }

    public ApiClient getCMClient(String host, Integer port, String version, String username, String password) {
        return getCMClient(host, port, version, username, password, false, "");
    }


    public String getBaseUrl(String host, Integer port, String version, Boolean isSSLEnabled) {
        String protocol = "http";
        if (isSSLEnabled) {
            protocol = "https";
        }
        return protocol + "://" + host + ":" + port + "/api/" + version;
    }


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

    public ApiImpalaQueryDetailsResponse queryDetailRaw(String clusterName, String serviceName, String queryId) throws ApiException {
        ApiImpalaQueryDetailsResponse result;

        ImpalaQueriesResourceApi apiInstance = new ImpalaQueriesResourceApi(cmClient);

        if(LOGGER.isDebugEnabled()) {
            LOGGER.debug(clusterName + "," + serviceName + "," + queryId);
        }
        result = apiInstance.getQueryDetails(clusterName, queryId, serviceName, "text");

        return result;
    }

    public ApiImpalaQueryDetailsResponse queryDetailThroughHTTP(String clusterName, String serviceName, String queryId) {
        String auth = username + ":" + password;
        byte[] encodedAuth = Base64.getUrlEncoder().encode(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);

        CloseableHttpClient client = HttpClients.createDefault();

        String baseUrl = getBaseUrl(host, port, version, false);

        HttpGet get = new HttpGet(baseUrl + "/clusters/" + clusterName + "/services/" + serviceName + "/impalaQueries/" + queryId);

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

    public QuerySearchResult query(String clusterName, String serviceName, String filter, String from, String to) throws ApiException {
        return new QuerySearchResult(cmClient, clusterName, serviceName, filter, from, to);
    }

}
