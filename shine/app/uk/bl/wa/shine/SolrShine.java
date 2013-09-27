/**
 * 
 */
package uk.bl.wa.shine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import play.Logger;


/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrShine {

	private SolrServer solr = null;
	
	public SolrShine(String host) {
		 solr = new HttpSolrServer(host);		
	}
	
	public QueryResponse search( String query, Map<String,List<String>> params ) throws SolrServerException {
		SolrQuery parameters = new SolrQuery();
		parameters.set("q", query);
		parameters.addFacetField(
				"content_encoding",
				"content_ffb",
				"content_language",
				"domain",
				"author",
				"content_type",
				"content_type_norm",
				"crawl_year",
				"content_type_ext",
				"links_public_suffixes",
				"generator",
				"last_modified_year",
				"content_type_full",
				"postcode_district",
				"sentiment",
				"public_suffix");
		parameters.setFacetMinCount(1);
		List<String> fq = new ArrayList<String>();
		for( String param : params.keySet() ) {
			fq.add(param+":"+params.get(param).get(0));
		}
		if( fq.size() > 0 ) {
			parameters.setFilterQueries(fq.toArray(new String[fq.size()]));
		}
		parameters.setSort("crawl_date", ORDER.asc);
		return solr.query(parameters);
	}
	
	private String temp( String query ) throws SolrServerException {
		QueryResponse res = this.search(query, null);
		res.getFacetFields().get(0).getValues().get(0).getName();
		res.getResults().get(0).getFirstValue("title");
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
