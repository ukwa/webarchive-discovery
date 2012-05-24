/**
 * 
 */
package uk.bl.wap.hadoop.entities;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InternetDomainName;

import uk.bl.wap.hadoop.WritableArchiveRecord;

/**
 * @author AnJackson
 *
 */
public class LinkExtractor {
	

	/**
	 * Extract a href links from the given WritableArchiveRecord.
	 * 
	 * @param rec
	 * @return
	 * @throws IOException
	 */
	public static Set<String> extractLinks( WritableArchiveRecord rec, boolean includeImgLinks ) throws IOException {
		return extractLinks( 
				new ByteArrayInputStream( rec.getPayload() ), 
				null, 
				rec.getRecord().getHeader().getUrl(),
				includeImgLinks );
	}
	
	/**
	 * Use a tolerant parser to extract all of the a href links from a document.
	 * 
	 * Does not extract other links, e.g. stylesheets, etc. etc. Image links optional.
	 * 
	 * @param input The InputStream
	 * @param charset The character set, e.g. "UTF-8". Value of "null" attempts to extract encoding from the document and falls-back on UTF-8.
	 * @param baseUri base URI for the page, for resolving relative links. e.g. "http://www.example.com/"
	 * @return
	 * @throws IOException
	 */
	public static Set<String> extractLinks( InputStream input, String charset, String baseUri, boolean includeImgLinks ) throws IOException {
		Set<String> linkset = new HashSet<String>();
		
		Document doc = Jsoup.parse(input, charset, baseUri );

		// All a with href
		for( Element link : doc.select("a[href]") ) {
			linkset.add( link.attr("href") );
		}
		// All images:
		if( includeImgLinks ) {
			for( Element link : doc.select("img[src]") ) {
				linkset.add( link.attr("src") );
			}
		}
		// Example of use: all PNG references...
		//Elements pngs = doc.select("img[src$=.png]");

		//Element masthead = doc.select("div.masthead").first();
		return linkset;
	}

	/**
	 * 
	 * @param input
	 * @param charset
	 * @param baseUri
	 * @param includeImgLinks
	 * @return
	 * @throws IOException
	 */
	public static Set<String> extractPublicSuffixes( InputStream input, String charset, String baseUri, boolean includeImgLinks ) throws IOException {
		Set<String> links = extractLinks(input, charset, baseUri, includeImgLinks);
		Set<String> suffixes = new HashSet<String>();
		for( String link : links ) {
			String suffix = extractPublicSuffix(link);
			if( suffix != null ) {
				suffixes.add(suffix);
			}
		}
		return suffixes;
	}
	
	/**
	 * Extract the public suffix, but compensate for the fact that the library we are 
	 * using considers 'uk' to be the public suffix, rather than e.g. 'co.uk'
	 * 
	 * @param url e.g. http://this.that.google.com/tootles
	 * @return e.g. "com", or "co.uk".  NULL if there was a parsing error.
	 */
	public static String extractPublicSuffix( String url ) {
		String host;
		try {
			host = new URI(url).getHost();
		} catch (URISyntaxException e) {
			return null;
		}
		// Parse out the public suffix
		InternetDomainName domainName = InternetDomainName.fromLenient(host);
		InternetDomainName suffix = null;
		if( host.endsWith(".uk")) {
			ImmutableList<String> parts = domainName.parts();
			if( parts.size() >= 2 ) {
				suffix = InternetDomainName.fromLenient( parts.get(parts.size()-2) +"."+ parts.get(parts.size()-1) );
			}
		} else {
			suffix = domainName.publicSuffix();
		}
		// Return a value:
		if( suffix == null ) return null;
		return suffix.name();
	}
	
	
	public static void main( String[] args ) {
		System.out.println("TEST: "+extractPublicSuffix("http://www.google.com/test.html"));
		System.out.println("TEST: "+extractPublicSuffix("http://www.google.co.uk/test.html"));
		System.out.println("TEST: "+extractPublicSuffix("http://www.google.sch.uk/test.html"));
		System.out.println("TEST: "+extractPublicSuffix("http://www.google.nhs.uk/test.html"));
		System.out.println("TEST: "+extractPublicSuffix("http://www.nationalarchives.gov.uk/test.html"));
		System.out.println("TEST: "+extractPublicSuffix("http://www.bl.uk/test.html"));
	}

}
