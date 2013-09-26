/**
 * 
 */
package uk.bl.wa.shine;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;


/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrShine {

	private SolrServer solr = null;
	
	public SolrShine(String host) {
		 solr = new HttpSolrServer(host);		
	}
	
	public String search( String query ) throws SolrServerException {
		SolrQuery parameters = new SolrQuery();
		parameters.set("q", query);
		QueryResponse response = solr.query(parameters);
		SolrDocumentList list = response.getResults();
		if( list.size() == 0 ) return "";
		return list.get(0).getFieldValue("title").toString();
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
