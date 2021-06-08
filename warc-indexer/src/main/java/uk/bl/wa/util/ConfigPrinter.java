/**
 * 
 */
package uk.bl.wa.util;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

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
