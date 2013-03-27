package uk.bl.wa.parsers;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class HtmlFeatureParser extends AbstractParser {

	/** */
	private static final long serialVersionUID = 1631417895901342813L;

	private static Log log = LogFactory.getLog(HtmlFeatureParser.class);
	
	public static final String ORIGINAL_PUB_DATE = "OriginalPublicationDate";
	public static final String LINK_LIST = "LinkList";
	public static final String FIRST_PARAGRAPH = "FirstParagraph";
	
	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void parse(InputStream stream, ContentHandler handler,
			Metadata metadata, ParseContext context) throws IOException,
			SAXException, TikaException {
		// Pick up the URL:
		String url = metadata.get( Metadata.RESOURCE_NAME_KEY );
		
		// Parse it using JSoup
		Document doc = Jsoup.parse(stream, null, url );

		// Get the links (no image links):
		Set<String> links = this.extractLinks(doc, false);
		if( links != null && links.size() > 0 )
			metadata.set(LINK_LIST, StringUtils.join(links, " "));
		
		// Get the publication date, from BBC pages:
		for( Element meta : doc.select("meta[name=OriginalPublicationDate]") ) {
			metadata.set(ORIGINAL_PUB_DATE, meta.attr("content"));
			//log.debug(ORIGINAL_PUB_DATE + ": " + meta.attr("content"));
		}
		
		// Grab the first paragraph with text, and extract the text:
		for( Element p : doc.select("p") )  {
			String pt = p.text();
			if( pt != null ) {
				pt = pt.trim();
				if( pt.length() > 0 ) {
					metadata.set(FIRST_PARAGRAPH, p.text() );
					//log.debug(FIRST_PARAGRAPH + ": " +p.text() );
					break;
				}
			}
		}
	}
	

	/**
	 * Use a tolerant parser to extract all of the absolute a href links from a document.
	 * 
	 * Does not extract other links, e.g. stylesheets, etc. etc. Image links optional.
	 * 
	 * @param input The InputStream
	 * @param charset The character set, e.g. "UTF-8". Value of "null" attempts to extract encoding from the document and falls-back on UTF-8.
	 * @param baseUri base URI for the page, for resolving relative links. e.g. "http://www.example.com/"
	 * @return
	 * @throws IOException
	 */
	private Set<String> extractLinks( Document doc, boolean includeImgLinks ) throws IOException {
		Set<String> linkset = new HashSet<String>();
		
		// All a with href
		for( Element link : doc.select("a[href]") ) {
			linkset.add( link.attr("abs:href") );
		}
		// All images:
		if( includeImgLinks ) {
			for( Element link : doc.select("img[src]") ) {
				linkset.add( link.attr("abs:src") );
			}
		}
		// Example of use: all PNG references...
		//Elements pngs = doc.select("img[src$=.png]");

		//Element masthead = doc.select("div.masthead").first();
		return linkset;
	}
	
	/**
	 * Quick test.
	 * @throws TikaException 
	 * @throws SAXException 
	 * @throws IOException 
	 */
	public static void main( String[] argv ) throws IOException, SAXException, TikaException  {
		URL url = new URL("http://www.bbc.co.uk/news/magazine-21351017");
		HtmlFeatureParser hfp = new HtmlFeatureParser();
		Metadata metadata = new Metadata();
		metadata.set( Metadata.RESOURCE_NAME_KEY, url.toString());
		hfp.parse(url.openStream(), null, metadata, null);

	}
	
	public static Metadata extractMetadata( InputStream in, String url ) {
		HtmlFeatureParser hfp = new HtmlFeatureParser();
		Metadata metadata = new Metadata();
		metadata.set(Metadata.RESOURCE_NAME_KEY, url);
		try {
			hfp.parse(in, null, metadata, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TikaException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return metadata;
	}
}
