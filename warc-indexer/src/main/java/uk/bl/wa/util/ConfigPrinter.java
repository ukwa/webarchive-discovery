/**
 * 
 */
package uk.bl.wa.util;

import java.io.File;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 */
public class ConfigPrinter {

	/**
	 * Print any Config.
	 * @param config
	 */
	public static void print( Config config ) {
		// Set up to avoid printing internal details:
		ConfigRenderOptions options = ConfigRenderOptions.defaults().setOriginComments( false );

		// Print the standard config to STDOUT:
		System.out.println( config.withOnlyPath( "warc" ).root().render( options ) );
	}

	/**
	 * Path to config. file can be passed, otherwise the defaults will be read.
	 * @param args
	 */
	public static void main( String[] args ) {
		// Load the config:
		Config config;
		if( args.length > 0 ) {
			config = ConfigFactory.parseFile( new File( args[ 0 ] ) );
		} else {
			config = ConfigFactory.load();
		}
		print( config );
	}
}
