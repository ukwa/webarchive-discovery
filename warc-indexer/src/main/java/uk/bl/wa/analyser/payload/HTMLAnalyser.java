/**
 * 
 */
package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
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

import java.io.InputStream;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.httpclient.URIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;
import org.archive.url.SURT;

import com.typesafe.config.Config;

import uk.bl.wa.extract.LinkExtractor;
import uk.bl.wa.indexer.WARCIndexer;
import uk.bl.wa.parsers.HtmlFeatureParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;
import uk.bl.wa.util.TimeLimiter;

/**
 * @author anj
 *
 */
public class HTMLAnalyser extends AbstractPayloadAnalyser {
    private static Logger log = LoggerFactory.getLogger( HTMLAnalyser.class );

    private HtmlFeatureParser hfp;
    private boolean extractLinkDomains;
    private boolean extractLinkHosts;
    private boolean extractLinks;
    private boolean extractElementsUsed;
    private boolean extractImageLinks;
    private boolean normaliseLinks;

    public HTMLAnalyser() {
    }

    public HTMLAnalyser( Config conf ) {
        this.configure(conf);
    }

    @Override
    public void configure(Config conf) {
        this.extractLinks = conf.getBoolean( "warc.index.extract.linked.resources" );
        log.info("HTML - Extract resource links " + this.extractLinks);
        this.extractLinkHosts = conf.getBoolean( "warc.index.extract.linked.hosts" );
        log.info("HTML - Extract host links " + this.extractLinkHosts);
        this.extractLinkDomains = conf.getBoolean( "warc.index.extract.linked.domains" );
        log.info("HTML - Extract domain links " + this.extractLinkDomains);
        this.extractElementsUsed = conf.getBoolean( "warc.index.extract.content.elements_used" );
        log.info("HTML - Extract elements used " + this.extractElementsUsed);
        this.extractImageLinks = conf.getBoolean( "warc.index.extract.linked.images" );
        log.info("HTML - Extract image links " + this.extractElementsUsed);
        normaliseLinks = conf.hasPath(uk.bl.wa.parsers.HtmlFeatureParser.CONF_LINKS_NORMALISE) ?
               conf.getBoolean(uk.bl.wa.parsers.HtmlFeatureParser.CONF_LINKS_NORMALISE) :
                uk.bl.wa.parsers.HtmlFeatureParser.DEFAULT_LINKS_NORMALISE;

        hfp = new HtmlFeatureParser(conf);
    }

    @Override
    public boolean shouldProcess(String mime) {
        if (mime.startsWith("text")
                || mime.startsWith("application/xhtml+xml")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     *  JSoup link extractor for (x)html, deposit in 'links' field.
     * 
     * @param header
     * @param tikainput
     * @param solr
     */
    @Override
    public void analyse(String source, ArchiveRecordHeader header,
            InputStream tikainput, SolrRecord solr) {
        final long start = System.nanoTime();
        Metadata metadata = new Metadata();
        Set<String> hosts = new HashSet<String>();
        Set<String> suffixes = new HashSet<String>();
        Set<String> domains = new HashSet<String>();
        
        // JSoup NEEDS the URL to function:
        metadata.set( Metadata.RESOURCE_NAME_KEY, Normalisation.sanitiseWARCHeaderValue(header.getUrl()) );
        ParseRunner parser = new ParseRunner( hfp, tikainput, metadata, solr );
        try {
            TimeLimiter.run(parser, 30000L, false);
        } catch( Exception e ) {
            log.error( "WritableSolrRecord.extract(): " + e.getMessage() );
            solr.addParseException("when parsing as HTML", e);
        }
        Instrument.timeRel("HTMLAnalyzer.analyze#total", "HTMLAnalyzer.analyze#parser", start);

        
        //Process image links
        if (extractImageLinks){
          String[] imageLinks = metadata.getValues( HtmlFeatureParser.IMAGE_LINKS );
          if (imageLinks != null){            
            for( String link : imageLinks ) { 
              String urlNorm  = normaliseLinks ? Normalisation.canonicaliseURL(link) : link;
              solr.addField( SolrFields.SOLR_LINKS_IMAGES, urlNorm);
            }
          }                
        }
        
        // Process links:
        String[] links_list = metadata.getValues( HtmlFeatureParser.LINK_LIST );
        if( links_list != null ) {
            String lhost, ldomain, lsuffix;
            for( String link : links_list ) {
                lhost = LinkExtractor.extractHost( link );
                if( !lhost.equals( LinkExtractor.MALFORMED_HOST ) ) {
                    hosts.add(lhost);
                }
                lsuffix = LinkExtractor.extractPublicSuffix( link );
                if( lsuffix != null ) {
                    suffixes.add(lsuffix);
                }
                ldomain = LinkExtractor.extractPrivateSuffix( link );
                if( ldomain != null ) {
                    domains.add(ldomain);
                }
                // Also store actual resource-level links:
                if( this.extractLinks ){
                    String urlNorm  = normaliseLinks ? Normalisation.canonicaliseURL(link) : link;
                    solr.addField( SolrFields.SOLR_LINKS, urlNorm );
                }
            }
            // Store the data from the links:
            if( this.extractLinkHosts ) {
                Set<String> cHostSet = new HashSet<String>();
                for (String host: hosts) {
                    // Canonicalise the host:
                    String cHost = host;
                    if (WARCIndexer.CANONICALISE_HOST) {
                        try {
                            cHost = Normalisation.canonicaliseHost(host);
                        } catch (URIException e) {
                            log.error("Failed to canonicalise host: '" + host
                                    + "': " + e);
                            cHost = host;
                        }
                    }
                    cHostSet.add(cHost);
                }

                Set<String> uniqueHostLevels = new LinkedHashSet<String>();

                // Store the unique hosts:
                for (String host : cHostSet) {
                    solr.addField(SolrFields.SOLR_LINKS_HOSTS, host);
                    uniqueHostLevels.addAll(LinkExtractor.allLevels(host));
                }

                for(String hostLevel : uniqueHostLevels) {
                    solr.addField(SolrFields.SOLR_LINKS_HOSTS_SURTS,
                            SURT.toSURT(hostLevel));
                }
            }
            if( this.extractLinkDomains ) {
                for (String domain: domains) {
                    solr.addField( SolrFields.SOLR_LINKS_DOMAINS, domain );
                }
            }
            for (String suffix: suffixes) {
                solr.addField( SolrFields.SOLR_LINKS_PUBLIC_SUFFIXES, suffix );
            }
        }
        // Process element usage:
        if( this.extractElementsUsed ) {
            String[] de = metadata.getValues( HtmlFeatureParser.DISTINCT_ELEMENTS );
            if( de != null ) {
                for( String e : de ) {
                    solr.addField( SolrFields.ELEMENTS_USED, e );
                }
            }
        }
        for( String lurl : metadata.getValues( Metadata.LICENSE_URL ) ) {
            solr.addField( SolrFields.LICENSE_URL, lurl );
        }
        Instrument.timeRel("WARCPayloadAnalyzers.analyze#total", "HTMLAnalyzer.analyze#total", start);
    }
    
}
