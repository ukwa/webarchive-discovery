/**
 * 
 */
package uk.bl.wa.shine;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.FacetField.Count;

import play.Configuration;
import play.Logger;
import uk.bl.wa.shine.URIStatusLookup.URIStatus;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Rescued extends SolrShine {
	
	URIStatusLookup usl = new URIStatusLookup();

	/*
With SolrJ you don't need to urlencode. However you still need to escape 
special query syntax parameters. Can you try this:

SolrQuery.addFilterQuery("yourStringField:Cameras\\ \\&\\ Photos")
	 * 
	 * 
	 */
	
	public Rescued(String host, Configuration config) {
		super(host, config);
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
		int total = 20;
		String[] urls = new String[total];
		for( int i = 0; i < total; i++ ) {
			String url = getRandomItemForFacet(years.get(rng.nextInt(years.size())));
			urls[i] = url;
			Logger.info("Got URL: "+url);
		}
		URIStatus[] statuses = usl.getStatus(urls);
		Map<URIStatus,Integer> counts = new HashMap<URIStatus,Integer>();
		for( URIStatus status : statuses ) {
			if( ! counts.containsKey(status) ) {
				counts.put(status, 1);
			} else {
				counts.put(status, counts.get(status) + 1);
			}
		}
		for( URIStatus status : counts.keySet() ) {
			Logger.info("Status Count: "+status+" "+counts.get(status));
		}
		return "";
	}
	
	/**
	 * <date name="wct_harvest_date">2011-06-23T09:06:29Z</date>
	 * http://chrome.bl.uk:8080/solr/select/?q=*:*&rows=1&sort=random_2%20desc
	 * THEN wct_url (or just url for other indexes)
	 * @param fq
	 * @return
	 * @throws SolrServerException
	 * @throws MalformedURLException
	 */
	private String getRandomItemForFacet(String fq) throws SolrServerException, MalformedURLException {
		Random rng = new Random();
		SolrQuery q = new SolrQuery();
		q.set("q", "*:*");
		q.setFilterQueries(fq);
		q.setSort("random_"+rng.nextInt(Integer.MAX_VALUE),ORDER.desc);
		QueryResponse res = solr.query(q);
		Logger.info("FQ: "+fq);
		Logger.info("Results: "+res.getResults().getNumFound());
		long target = (long) (rng.nextDouble()*res.getResults().getNumFound());
		Logger.info("Target:"+target);
		q.setRows(1);
		q.setStart((int) target); // FIXME Integer, not Long! Should be ok?
		res = solr.query(q);
		String url = res.getResults().get(0).getFirstValue("url").toString();
		String domain = res.getResults().get(0).getFirstValue("domain").toString();
		Logger.info("GOT: "+domain+ " > "+ url);
		return url;
	}
		


	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}

}
