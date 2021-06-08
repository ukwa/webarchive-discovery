package uk.bl.wa.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;

import com.typesafe.config.Config;

import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Solr Server Wrapper
 * 
 * @author anj
 */
public class SolrWebServer {
    private static Logger log = LoggerFactory.getLogger(SolrWebServer.class);

    @Command(name = "solr-options", description = "Setting up the Solr connection.")
    public
    static class SolrOptions {
        // Must specify either one or more Solr endpoints, or Zookeeper hosts
        // and a Solr collection
        @ArgGroup(exclusive = true, multiplicity = "1")
        SolrServiceOptions service;

        public
        static class SolrServiceOptions {
            @Option(names = { "-S",
                    "--solr-endpoint" }, description = "The HTTP Solr endpoints to use. Set this multiple times to use a load-balanced group of endpoints.")
            String[] endpoint;

            @ArgGroup(exclusive = false)
            ZKCollection zk;
        }

        public
        static class ZKCollection {
            @Option(names = { "-Z",
                    "--solr-zookeepers" }, description = "A list of comma-separated HOST:PORT values for the Zookeepers. e.g. 'zk1:2121,zk2:2121'")
            String zookeepers;

            @Option(names = { "-C",
                    "--solr-collection" }, description = "The Solr collection to populate.")
            String collection;
        }

        @Option(names = "--solr-batch-size", description = "Size of batches of documents to send to Solr.", defaultValue = "100")
        public int batchSize;

    }

    public static void main(String[] args) {
        SolrOptions cmd = new SolrOptions();
        new CommandLine(cmd).usage(System.out);
    }

    private SolrClient solrServer;
    
    /**
     * Initializes the Solr connection
     */
    public SolrWebServer(SolrOptions opts) {
        // Setup based on options:
        if (opts.service.endpoint != null) {
            if (opts.service.endpoint.length == 1) {
                log.info("Setting up HttpSolrServer client from a url: "
                        + opts.service.endpoint[0]);
                solrServer = new HttpSolrClient.Builder(opts.service.endpoint[0]).build();
            } else {
                log.info(
                        "Setting up LBHttpSolrServer client from servers list: "
                                + opts.service.endpoint);
                solrServer = new LBHttpSolrClient.Builder().withBaseSolrUrls(opts.service.endpoint).build();
            }
        } else {
        	
        	final List<String> zkHosts = Arrays.asList(opts.service.zk.zookeepers.split(","));
            log.info("Setting up CloudSolrServer client via zookeepers: "+zkHosts);
            solrServer = new CloudSolrClient.Builder(zkHosts, Optional.empty()).build();
            ((CloudSolrClient) solrServer)
                    .setDefaultCollection(opts.service.zk.collection);
        }

        if (solrServer == null) {
            throw new RuntimeException("Cannot connect to Solr Server!");
        }
    }

    public static final String CONF_ZOOKEEPERS = "warc.solr.zookeepers";

    public static final String CONF_HTTP_SERVERS = "warc.solr.servers";

    public static final String CONF_HTTP_SERVER = "warc.solr.server";

    public static final String COLLECTION = "warc.solr.collection";

    public SolrWebServer(Config conf) {

        if( conf.hasPath(CONF_HTTP_SERVER)) {
            log.info("Setting up HttpSolrServer client from a url: "+conf.getString(CONF_HTTP_SERVER));
            solrServer = new HttpSolrClient.Builder(
                    conf.getString(CONF_HTTP_SERVER)).build();
            
        } else if (conf.hasPath(CONF_ZOOKEEPERS)) {
        	final List<String> zkHosts = Arrays.asList(conf.getString(CONF_ZOOKEEPERS).split(","));
            log.info("Setting up CloudSolrServer client via zookeepers: "+zkHosts);
            solrServer = new CloudSolrClient.Builder(zkHosts, Optional.empty()).build();
            ((CloudSolrClient) solrServer)
                    .setDefaultCollection(conf
                    .getString(COLLECTION));
            
        } else if (conf.hasPath(CONF_HTTP_SERVERS)) {
            log.info("Setting up LBHttpSolrServer client from servers list.");
            solrServer = new LBHttpSolrClient.Builder().withBaseSolrUrls(
                    conf.getString(
                    CONF_HTTP_SERVERS).split(",")).build();
            
        } else {
            log.error("No valid SOLR config found.");
        }

        if (solrServer == null) {
            System.out.println("Cannot connect to Solr Server!");
        }
    }

    public SolrClient getSolrServer() {
        return this.solrServer;
    }

    /**
     * Post a List of docs.
     * 
     * @param solrDoc
     * @return 
     * @throws SolrServerException
     * @throws IOException
     */
    public  UpdateResponse add(List<SolrInputDocument> docs)
            throws SolrServerException, IOException {
        /*
         * for (SolrInputDocument doc : docs) { log.info("DOC:" +
         * doc.toString()); solrServer.add(doc); } return null;
         */
        return solrServer.add(docs);
    }

    /**
     * Post a single documents.
     * 
     * @param solrDoc
     * @throws SolrServerException
     * @throws IOException
     */
    public void updateSolrDoc(SolrInputDocument doc)
            throws SolrServerException, IOException {
        solrServer.add(doc);

    }

    /**
     * Commit the SolrServer.
     * 
     * @throws SolrServerException
     * @throws IOException
     */
    public void commit() throws SolrServerException, IOException {

        solrServer.commit();

    }

    /**
     * Sends the prepared query to solr and returns the result;
     * 
     * @param query
     * @return
     * @throws SolrServerException
     * @throws IOException
     */

    public QueryResponse query(SolrQuery query)
            throws SolrServerException, IOException {
        QueryResponse rsp = solrServer.query(query);
        return rsp;
    }

    /**
     * Overrides the generic destroy method. Closes all Solrj connections.
     */
    public void destroy() {
        solrServer = null;
    }
}
