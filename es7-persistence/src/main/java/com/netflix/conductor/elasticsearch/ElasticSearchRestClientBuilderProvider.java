package com.netflix.conductor.elasticsearch;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Provider;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

public class ElasticSearchRestClientBuilderProvider implements Provider<RestClientBuilder> {
    private final ElasticSearchConfiguration configuration;

    @Inject
    public ElasticSearchRestClientBuilderProvider(ElasticSearchConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public RestClientBuilder get() {
        RestClientBuilder builder = RestClient.builder(convertToHttpHosts(configuration.getURIs()));

        setAuthentication(builder);

        return builder;
    }

    private void setAuthentication(RestClientBuilder restClientBuilder) {

        String password = configuration.getElasticsearchPassword();
        if (password == null) {
            return;
        }
        String username = configuration.getElasticsearchUsername();

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            httpClientBuilder.setKeepAliveStrategy((response, context) -> {
                HeaderElementIterator it = new BasicHeaderElementIterator(response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && "timeout".equalsIgnoreCase(param)) {
                        return Long.parseLong(value) * 1000;
                    }
                }
                return 5 * 60 * 1000;
            });
            return httpClientBuilder;
        });

    }

        private HttpHost[] convertToHttpHosts(List<URI> hosts) {
        List<HttpHost> list = hosts.stream()
                .map(host -> new HttpHost(host.getHost(), host.getPort(), host.getScheme()))
                .collect(Collectors.toList());

        return list.toArray(new HttpHost[list.size()]);
    }
}
