/**
 * 
 */
package uk.bl.wa.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ConfigPrinter {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Config config = ConfigFactory.load();
		//System.out.println(config.getValue("warc").render());
		System.out.println(config.withOnlyPath("warc").root().render());
	}

}
