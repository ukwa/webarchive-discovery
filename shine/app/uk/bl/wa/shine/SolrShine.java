/**
 * 
 */
package uk.bl.wa.shine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;

import play.Logger;


/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrShine extends Solr {

	private List<String> facets = null;
	
	private Map<String,String> facet_names = null;
	
	private Map<String,List<String>> facets_tree = null;
	
	public SolrShine( play.Configuration config ) {
		 super(config);
		 //
		 this.facets = new ArrayList<String>();
		 this.facet_names = new HashMap<String,String>();
		 this.facets_tree = new LinkedHashMap<String,List<String>>();
		 for( String fc : config.getConfig("facets").subKeys() ) {
			 List<String> fl = new ArrayList<String>();
			 for( String f : config.getConfig("facets."+fc).subKeys() ) {
				 fl.add(f);
				 // Also store in a flat list:
				 this.facets.add(f);
				 // Also store the name:
				 this.facet_names.put(f,config.getString("facets."+fc+"."+f));
			 }
			 Logger.info("Putting "+fc+" > "+fl);
			 this.facets_tree.put(fc, fl);
		 }
    }
	
	public QueryResponse search( String query, Map<String,List<String>> params ) throws SolrServerException {
		SolrQuery parameters = new SolrQuery();
		// The query:
		parameters.set("q", query);
		// Facets:
		for( String f : facets ) {
			parameters.addFacetField("{!ex="+f+"}"+f);
		}
		parameters.setFacetMinCount(1);
		List<String> fq = new ArrayList<String>();
		for( String param : params.keySet() ) {
			String field = param;
			if( field.startsWith("-")) field = field.replaceFirst("-", "");
			String filter = "{!tag="+field+"}"+param+":";
			int counter = 0;
			for( String val : params.get(param)) {
				if( counter > 0 ) filter += " OR ";
			    filter += val;// TODO Escape correctly?
			    counter++;
			    
			}
			fq.add(filter);
		}
		if( fq.size() > 0 ) {
			parameters.setFilterQueries(fq.toArray(new String[fq.size()]));
		}
		// Sorts:
		parameters.setSort("wayback_date", ORDER.asc);
		//parameters.setSort("sentiment_score", ORDER.asc);
		// Paging:
		parameters.setRows(20);
		Logger.info("Query: "+parameters.toString());		
		// Perform the query:
		QueryResponse res = solr.query(parameters);
		Logger.info("QTime: "+res.getQTime());
		Logger.info("Response Header: "+res.getResponseHeader());
		return res;
	}


	
	private String temp( String query ) throws SolrServerException {
		QueryResponse res = this.search(query, null);
		res.getFacetFields().get(0).getValues().get(0).getName();
		res.getResults().get(0).getFirstValue("title");
		res.getResults().getNumFound();
		return null;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
	}

}
