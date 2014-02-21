
package uk.bl.wa.util.solr;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2014 The UK Web Archive
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
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;


/**
 * Solr Server Wrapper
 * 
 * @author JoeOBrien
 */
public class SolrWebServer {
	
	private SolrServer solrServer;	
	
	/**
	 * Initializes the Solr connection
	 */
	public SolrWebServer( String solrUrl) {
		
		if(solrUrl == null||solrUrl.isEmpty()){
			System.out.println("Solr URL Not defined");
		}

		solrServer = new HttpSolrServer(solrUrl);
		
		if(solrServer == null){
			System.out.println("Cannot connect to Solr Server: " + solrUrl);
		}
	}

	/**
	 * Post a List of docs.
	 * 
	 * @param solrDoc
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void updateSolr(List<SolrInputDocument> docs) throws SolrServerException, IOException{
		
			solrServer.add(docs);
		
	}
	
	/**
	 * Post a single documents.
	 * 
	 * @param solrDoc
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void updateSolrDoc(SolrInputDocument doc) throws SolrServerException, IOException{
		
			solrServer.add(doc);	
		
	}
	
	/**
	 * Commit the SolrServer.
	 * 
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void commit() throws SolrServerException, IOException{
		
			solrServer.commit();
		
	}
	
	/**
	 * Sends the prepared query to solr and returns the result;
	 * @param query
	 * @return
	 * @throws SolrServerException 
	 */
	
	public QueryResponse execQuery(SolrQuery query) throws SolrServerException {
		QueryResponse rsp = solrServer.query( query );
		
		return rsp;
	}
	
	
	/**
	 * Overrides the generic destroy method. Closes all Solrj connections.
	 */
	public void destroy() {
		solrServer = null;
	}
}
