/**
 * 
 */
package uk.bl.wa.analyser.text;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2020 The webarchive-discovery project contributors
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

import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.solr.SolrWebServer.SolrOptions;

/**
 * @author anj
 *
 */
@Command(description = "Extracted text analysis options")
public abstract class AbstractTextAnalyser {

    protected boolean enabled = false;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Sub-classes should implement this method to create text payload annotations for solr.
     * 
     * @param text
     * @param solr
     */
    public abstract void analyse( String text, SolrRecord solr );
    
    /**
     * This dynamically loads the available parser implementations on the
     * classpath. Passes in the provided configuration to get things set up.
     * 
     * @return
     */
    public static List<AbstractTextAnalyser> getTextAnalysers(CommandLine cli) {

        // load our plugins
        ServiceLoader<AbstractTextAnalyser> serviceLoader = ServiceLoader
                .load(AbstractTextAnalyser.class);

        // Get the list:
        List<AbstractTextAnalyser> providers = new ArrayList<AbstractTextAnalyser>();
        for (AbstractTextAnalyser provider : serviceLoader) {
            /*
             * Hook for pulling options to add as a run-time mixin. See
             * https://picocli.info/#_adding_mixins_programmatically
             */
            cli.addMixin(provider.getClass().getCanonicalName(), provider);

            // Remember the provider
            providers.add(provider);
        }

        return providers;
    }

    /**
     * Just for testing.
     * 
     * @param ignored
     */
    public static void main(String[] ignored) {

        // Get the config:
        SolrOptions opts = new SolrWebServer.SolrOptions();
        CommandLine cli = new CommandLine(opts);

        // create a new provider and call getMessage()
        List<AbstractTextAnalyser> providers = AbstractTextAnalyser
                .getTextAnalysers(cli);
        for (AbstractTextAnalyser provider : providers) {
            System.out.println(provider.getClass().getCanonicalName()
                    + " Enabled: " + provider.isEnabled());
        }

        SolrRecordFactory.createFactory(cli);

        // And print usage:
        cli.usage(System.out);

        // Try a setup:
        cli.parseArgs("-S", "dummy", "--postcodes");

        // And check:
        for (AbstractTextAnalyser provider : providers) {
            System.out.println(provider.getClass().getCanonicalName()
                    + " Enabled: " + provider.isEnabled());
        }

    }
}
