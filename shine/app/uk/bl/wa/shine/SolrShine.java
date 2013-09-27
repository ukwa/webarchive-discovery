/**
 * 
 */
package uk.bl.wa.shine;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.FacetField.Count;
import org.apache.solr.client.solrj.response.QueryResponse;

import com.typesafe.config.ConfigValue;

import play.Logger;


/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrShine {

	private HttpSolrServer solr = null;
	
	private List<String> facets = null;
	
	private Map<String,String> facet_names = null;
	
	private Map<String,List<String>> facets_tree = null;
	
	public SolrShine(String host, play.Configuration config ) {
		 solr = new HttpSolrServer(host);
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
			fq.add("{!tag="+field+"}"+param+":"+params.get(param).get(0));// FIXME Not get(0)
		}
		if( fq.size() > 0 ) {
			parameters.setFilterQueries(fq.toArray(new String[fq.size()]));
		}
		// Sorts:
		parameters.setSort("crawl_date", ORDER.asc);
		//parameters.setSort("sentiment_score", ORDER.asc);
		// Perform the query:
		QueryResponse res = solr.query(parameters);
		Logger.info("QTime: "+res.getQTime());
		return res;
	}
	
	public String halflife() throws SolrServerException, MalformedURLException {
		SolrQuery q = new SolrQuery();
		q.set("q", "*:*");
		q.addFacetField("crawl_year");
		QueryResponse res = solr.query(q);
		FacetField axis = res.getFacetField("crawl_year");
		Logger.info("BASELINE"+axis.getValues());
		List<String> years = new ArrayList<String>();
		for( Count c : axis.getValues()) {
			years.add(c.getAsFilterQuery());
		}
		Logger.info("YEARS "+years);
		Random rng = new Random();
		getRandomItemForFacet(years.get(rng.nextInt(years.size())));
		return "";
	}
	
	private String getRandomItemForFacet(String fq) throws SolrServerException, MalformedURLException {
		SolrQuery q = new SolrQuery();
		q.set("q", "*:*");
		q.setFilterQueries(fq);
		QueryResponse res = solr.query(q);
		Logger.info("FQ: "+fq);
		Logger.info("Results: "+res.getResults().getNumFound());
		Random rng = new Random();
		long target = (long) (rng.nextDouble()*res.getResults().getNumFound());
		Logger.info("Target:"+target);
		q.setRows(1);
		q.setStart((int) target); // FIXME Integer, not Long! Should be ok?
		res = solr.query(q);
		String url = res.getResults().get(0).getFirstValue("url").toString();
		String domain = res.getResults().get(0).getFirstValue("domain").toString();
		Logger.info("GOT: "+domain+ " > "+ url);
		Logger.info("Domain Resolves: "+resolves(domain));
		Logger.info("Host Resolves: "+resolves(new URL(url).getHost()));
		Logger.info("URL Exists: "+exists(url));
		return "";
	}
	
	public static boolean resolves(String name) {
		try {
			InetAddress addr = InetAddress.getByName(name);
			Logger.info("addr:"+addr);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public static boolean exists(String URLName){
	    try {
	      HttpURLConnection.setFollowRedirects(false);
	      // note : you may also need
	      //        HttpURLConnection.setInstanceFollowRedirects(false)
	      HttpURLConnection con =
	         (HttpURLConnection) new URL(URLName).openConnection();
	      con.setRequestMethod("HEAD");
	      Logger.info("Status code: "+con.getResponseCode());
	      return (con.getResponseCode() == HttpURLConnection.HTTP_OK);
	    }
	    catch (Exception e) {
	       Logger.error("Lookup failed: "+e);
	       e.printStackTrace();
	       return false;
	    }
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
