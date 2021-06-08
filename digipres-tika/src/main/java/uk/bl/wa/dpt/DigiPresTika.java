/**
 * 
 */
package uk.bl.wa.dpt;

/*
 * #%L
 * digipres-tika
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
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.tika.Tika;

import uk.bl.wa.nanite.droid.DroidDetector;
import uk.gov.nationalarchives.droid.command.action.CommandExecutionException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class DigiPresTika {

    Tika t;
    DroidDetector dd;

    public DigiPresTika() throws CommandExecutionException {
        t = new Tika();
        dd = new DroidDetector();
        /*
         * Metadata metadata = new Metadata();
         * metadata.set(Metadata.RESOURCE_NAME_KEY, file.toURI().toString());
         * System.out.println("Result: " + dr.detect(new FileInputStream(file),
         * metadata));
         */
    }

    private void identify(String[] args) throws IOException {
        for (String path : args) {
            File file = new File(path);
            for (File in : FileUtils.listFiles(file, TrueFileFilter.TRUE,
                    TrueFileFilter.TRUE)) {
                String id;
                try {
                    id = t.detect(in);
                } catch (Throwable e) {
                    id = "application/x-error-"
                            + e.getMessage().replace(" ", "-").toLowerCase();
                }
                String did;
                try {
                    did = dd.detect(in).toString();
                } catch (Throwable e) {
                    did = "application/x-error-"
                            + e.getMessage().replace(" ", "-").toLowerCase();
                }
                System.out.println(in.getPath() + "\t" + id + "\t" + did);
            }
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws CommandExecutionException
     */
    public static void main(String[] args) throws IOException,
            CommandExecutionException {
        // create the command line parser
        CommandLineParser parser = new PosixParser();

        // create the Options
        Options options = new Options();
        options.addOption("a", "all", false,
                "do not hide entries starting with .");

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            // validate that block-size has been set
            if (line.hasOption("a")) {
                // print the value of block-size
                System.out.println(line.getOptionValue("a"));
            }

            DigiPresTika dpt = new DigiPresTika();
            dpt.identify(args);

        } catch (ParseException exp) {
            System.out.println("Unexpected exception:" + exp.getMessage());
            // automatically generate the help statement
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("ant", options);
        }
    }


}
