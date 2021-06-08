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

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.util.SurtPrefixSet;
import org.jetbrains.annotations.NotNull;
import uk.bl.wa.analyser.payload.ARCNameAnalyser;
import uk.bl.wa.annotation.Annotations;
import uk.bl.wa.annotation.Annotator;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;
import uk.bl.wa.solr.SolrWebServer;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;
import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Takes a list of WARC-names and performs matching on them, given af warc-indexer configuration file.
 *
 * Usage:
 * java -cp warc-indexer/target/warc-indexer-3.0.0-SNAPSHOT-jar-with-dependencies.jar
 *   uk.bl.wa.util.ValidateWARCNameMatchers -c /warc-indexer/src/test/resources/arcnameanalyser.conf -l warclist.dat
 */
public class ValidateWARCNameMatchers {
    
    private static Logger log = LoggerFactory.getLogger(ValidateWARCNameMatchers.class);

    private static final String CLI_USAGE = "-c config_file <File with list of WARC files>";
    private static final String CLI_HEADER =
            "ValidateWARCNameMatchers - Validates a list of WARC names against the name rule patterns in the " +
            "configuration";
    private static final String CLI_FOOTER = "";

    public static void main( String[] args ) throws IOException, TransformerFactoryConfigurationError {
        CommandLineParser parser = new PosixParser();
        String configFile = null;
        String warcs;
        boolean printLast = false;

        Options options = new Options();
        options.addOption("c", "config", true, "Configuration to use.");
        options.addOption("l", "print last matches", false,
                          "Print all the warc names that matches the last rule (which is normally the fallback rule).");

        try {
            // parse the command line arguments
            CommandLine line = parser.parse( options, args );
               String cli_args[] = line.getArgs();
           
        
            // Check that a mandatory Archive file(s) has been supplied
            if( !( cli_args.length == 1 ) ) {
                printUsage( options );
                System.exit( 0 );
            }
            warcs = cli_args[0];
            if (!new File(warcs).exists()) {
                throw new FileNotFoundException("The file with WARN names '" + warcs + "' does not exist");
            }

            if (line.hasOption("c")) {
                configFile = line.getOptionValue("c");
            }
            printLast = line.hasOption("l");

            validateRules(configFile, warcs, printLast);
        
        } catch (org.apache.commons.cli.ParseException e) {
            log.error("Parse exception when processing command line arguments", e);
        }
    }
    
    public static void validateRules(String configFile, String warcs, boolean printLast) throws IOException {
        long startTime = System.currentTimeMillis();

        List<ARCNameAnalyser.Rule> nameRules = getRules(configFile);
        validateRules(nameRules, new File(warcs), printLast);
        System.out.println("ValidateWARCNameMatchers Finished in " + ((System.currentTimeMillis() - startTime) / 1000.0)
                           + " seconds.");
    }

    @NotNull
    static List<ARCNameAnalyser.Rule> getRules(String configFile) {
        Config conf = getConfig(configFile);
        if (!conf.hasPath("warc.index.extract.content.arcname.rules")) {
            System.out.println("No rules for ARCNameAnalyzer at 'warc.index.extract.content.arcname.rules'; " +
                               "no processing of ARC names");
            System.exit(1);
        }
        List<ARCNameAnalyser.Rule> nameRules = new ARCNameAnalyser(conf).getRules();
        System.out.println("Resolved " + nameRules.size() + " WARC name rules");
        for (int i = 0 ; i < nameRules.size() ; i++) {
System.out.println("Pattern #" + i + ": '" + nameRules.get(i).pattern.pattern() + "'");
}
        return nameRules;
    }

    static void validateRules(List<ARCNameAnalyser.Rule> nameRules, File warcsFile, boolean printLast) throws IOException {
        validateRules(nameRules,
                      new BufferedReader(new InputStreamReader(new FileInputStream(warcsFile), "utf-8")),
                      printLast);
    }
    static void validateRules(List<ARCNameAnalyser.Rule> nameRules, BufferedReader warcs, boolean printLast)
            throws IOException {
        String warcName;
        final int matches[] = new int[nameRules.size()];
        final String lastMatches[] = new String[nameRules.size()];
        int total = 0;
        int nonMatches = 0;
        String lastNonMatch = null;
        while ((warcName = warcs.readLine()) != null) {
            total++;
            boolean match = false;
            for (int ruleIndex = 0 ; ruleIndex < nameRules.size() ; ruleIndex++) {
                if (nameRules.get(ruleIndex).pattern.matcher(warcName.trim()).matches()) {
                    matches[ruleIndex]++;
                    lastMatches[ruleIndex] = warcName;
                    match = true;
                    if (printLast && ruleIndex == nameRules.size()-1) {
                        System.out.println("Last rule match: " + warcName);
                    }
                    break;
                }
            }
            if (!match) {
                nonMatches++;
                lastNonMatch = warcName;
            }
        }

        for (int i = 0 ; i < nameRules.size() ; i++) {
            System.out.println(String.format("Rule #%d: %d warc name matches. Last match='%s'",
                                             i, matches[i],lastMatches[i]));
        }
        System.out.println("Total warc names: " + total);
        System.out.println("Total matching warc names: " + (total - nonMatches));
        System.out.println("Total non-matching warc names: " + nonMatches + ". Last non-match='" + lastNonMatch + "'");
    }

    private static Config getConfig(String configFile) {
        Config conf = ConfigFactory.load();
        if (configFile != null) {
            log.info("Loading config from log file: " + configFile);
            File configFilePath = new File(configFile);
            if (!configFilePath.exists()){
              log.error("Config file not found:"+configFile);
              System.exit( 0 );
            }

            conf = ConfigFactory.parseFile(configFilePath);
            // ConfigPrinter.print(conf);
            // conf.withOnlyPath("warc").root().render(ConfigRenderOptions.concise()));
            log.info("Loaded warc config " + conf.getString("warc.title"));
        }
        return conf;
    }

    private static void printUsage( Options options ) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.setWidth( 80 );
        helpFormatter.printHelp( CLI_USAGE, CLI_HEADER, options, CLI_FOOTER );
    }
    
}
