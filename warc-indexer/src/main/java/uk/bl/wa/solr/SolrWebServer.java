package uk.bl.wa.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2018 The webarchive-discovery project contributors
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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

/**
 * Solr Server Wrapper
 * 
 * @author anj
 */
public class SolrWebServer {
    private static Log log = LogFactory.getLog(SolrWebServer.class);

    private SolrClient solrServer;
    
    public static final String CONF_ZOOKEEPERS = "warc.solr.zookeepers";

    public static final String CONF_HTTP_SERVERS = "warc.solr.servers";

    public static final String CONF_HTTP_SERVER = "warc.solr.server";

    public static final String COLLECTION = "warc.solr.collection";

    public static final String NUM_SHARDS = "warc.solr.num_shards";

    public static final String HDFS_OUTPUT_PATH = "warc.solr.hdfs_output_path";

    /**
     * Initializes the Solr connection
     */
    public SolrWebServer(Config conf) {

        try {
            if( conf.hasPath(CONF_HTTP_SERVER)) {
                log.info("Setting up HttpSolrServer client from a url: "+conf.getString(CONF_HTTP_SERVER));
                solrServer = new HttpSolrClient(
                        conf.getString(CONF_HTTP_SERVER));
                
            } else if (conf.hasPath(CONF_ZOOKEEPERS)) {
                log.info("Setting up CloudSolrServer client via zookeepers.");
                solrServer = new CloudSolrClient(
                        conf.getString(CONF_ZOOKEEPERS));
                ((CloudSolrClient) solrServer)
                        .setDefaultCollection(conf
                        .getString(COLLECTION));
                
            } else if (conf.hasPath(CONF_HTTP_SERVERS)) {
                log.info("Setting up LBHttpSolrServer client from servers list.");
                solrServer = new LBHttpSolrClient(
                        conf.getString(
                        CONF_HTTP_SERVERS).split(","));
                
            } else {
                log.error("No valid SOLR config found.");
            }
        } catch (MalformedURLException e) {
            log.error("WARCIndexerReducer.configure(): " + e.getMessage());
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
