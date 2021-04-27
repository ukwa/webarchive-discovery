package uk.bl.wa.elastic;

/*-
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticClient {
    private static Logger log = LoggerFactory.getLogger(ElasticClient.class);
	public static final int DEFAULT_HTTPPORT = 9200;
	private RestHighLevelClient client;
	private String host;
	private String user;
	private String password;
	private String protocol;
	private int port;

	public ElasticClient(String aHost, String aUser, String aPassword, String aProtocol, int aPort) {
		host = aHost;
		user = aUser;
		password = aPassword;
		protocol = aProtocol;
		port = aPort;
	}
	
	public RestHighLevelClient getClient() {
		if (client == null) {
			synchronized (this) {
				if (client == null) {
					try {
						log.info("creating ElasticClient for " + host);
						
						RestClientBuilder builder = null;

			        	HttpHost httpHost = new HttpHost(InetAddress.getByName(host), port, protocol);
						
						if (user != null) {
							final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
							credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
							
							builder = RestClient.builder(httpHost)
							        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
							            @Override
							            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
							                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
							            }
							        })
							        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
							            @Override
							            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
							                return requestConfigBuilder.setConnectTimeout(5000)
							                        .setSocketTimeout(600000);
							            }
							        });
						}
						else {
							builder = RestClient.builder(httpHost)
							        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
							            @Override
							            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
							                return requestConfigBuilder.setConnectTimeout(5000)
							                        .setSocketTimeout(600000);
							            }
							        });
						}
						
						client = new RestHighLevelClient(builder);
						
					} catch (UnknownHostException e) {
						log.error(e.getMessage());
						throw new RuntimeException(e);
					}					
					log.info("created.");
				}
			}
		}
		return client;
	}

	public void close() {
		log.info("closing ElasticClient");
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
				log.error("Couldn't close ElasticClient");
			}
		}
		client = null;
		log.info("ElasticClient closed");
	}
}
