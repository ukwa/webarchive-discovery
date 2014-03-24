/**
 * 
 */
package uk.bl.wa.analyser.payload;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.typesafe.config.ConfigFactory;


/**
 * @author anj
 *
 */
public class WARCPayloadAnalysers {
	private static Log log = LogFactory.getLog( WARCPayloadAnalysers.class );
	
	public static HTMLAnalyser html = new HTMLAnalyser(ConfigFactory.load());

	public static PDFAnalyser pdf = new PDFAnalyser(ConfigFactory.load());

	public static XMLAnalyser xml = new XMLAnalyser(ConfigFactory.load());


}
