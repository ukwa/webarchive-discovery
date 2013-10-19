
package uk.bl.wa.util.solr;


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
	 * Post and Commit
	 * 
	 * @param solrDoc
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void updateSolr(List<SolrInputDocument> docs) throws SolrServerException, IOException{
		
			solrServer.add(docs);	
			solrServer.commit();
		
	}
	
	/**
	 * Post without Committing
	 * 
	 * @param solrDoc
	 * @throws SolrServerException
	 * @throws IOException
	 */
	public void updateSolrDoc(SolrInputDocument doc) throws SolrServerException, IOException{
		
			solrServer.add(doc);	
		
	}
	
	/**
	 * Post and Commit
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
