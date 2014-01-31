/**
 * 
 */
package uk.bl.wa.shine;

import java.util.Properties;

import org.apache.solr.client.solrj.impl.HttpSolrServer;

import play.Configuration;


/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public abstract class Solr {

	protected HttpSolrServer solr = null;
	
	public Solr( Configuration config ) {
		String host = config.getString("host");
		solr = new HttpSolrServer(host);
		// Set the proxy up:
		Properties systemProperties = System.getProperties();
		if( config.getString("http.proxyHost") != null ) {
			systemProperties.setProperty("http.proxyHost", config.getString("http.proxyHost") );
			systemProperties.setProperty("http.proxyPort", config.getString("http.proxyPort") );
		}
	}
	
}
