/**
 * 
 */
package uk.bl.wa.extract;

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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.tika.metadata.Metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.net.InternetDomainName;

import uk.bl.wa.parsers.HtmlFeatureParser;

/**
 * @author AnJackson
 *
 */
public class LinkExtractor {
    
    public static final String MALFORMED_HOST = "malformed.host";
    
    /**
     * 
     * @param url
     * @return
     */
    public static String extractHost(String url) {
        String host = "unknown.host";
        org.apache.commons.httpclient.URI uri = null;
        // Attempt to parse:
        try {
            uri = new org.apache.commons.httpclient.URI(url,false);
            // Extract domain:
            host = uri.getHost();
            if( host == null )
                host = MALFORMED_HOST;
        } catch ( Exception e ) {
            // Return a special hostname if parsing failed:
            host = MALFORMED_HOST;
        }
        return host;
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
    public static Set<String> extractPublicSuffixes( Metadata metadata ) throws IOException {
        String[] links = metadata.getValues(HtmlFeatureParser.LINK_LIST);
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
        return extractPublicSuffixFromHost(host);
    }
    
    public static String extractPublicSuffixFromHost( String host ) {
        if( host == null ) return null;
        // Parse out the public suffix:
        InternetDomainName domainName;
        try {
            domainName = InternetDomainName.from(host);
        } catch( Exception e ) {
            return null;
        }
        InternetDomainName suffix = null;
        if( host.endsWith(".uk")) {
            ImmutableList<String> parts = domainName.parts();
            if( parts.size() >= 2 ) {
                suffix = InternetDomainName.from(parts.get(parts.size() - 2)
                        + "." + parts.get(parts.size() - 1));
            }
        } else {
            suffix = domainName.publicSuffix();
        }
        // Return a value:
        if( suffix == null ) return null;
        return suffix.toString();
    }
    
    public static String extractPrivateSuffix( String url ) {
        String host;
        try {
            host = new URI(url).getHost();
        } catch (URISyntaxException e) {
            return null;
        }
        return extractPrivateSuffixFromHost(host);
    }

    /**
     * Attempt to parse out the private domain. Fall back on host if things go
     * awry.
     * 
     * @param host
     * @return
     */
    public static String extractPrivateSuffixFromHost( String host ) {
        if( host == null ) return null;
        // Parse out the public suffix:
        InternetDomainName domainName;
        try {
            domainName = InternetDomainName.from(host);
        } catch( Exception e ) {
            return host;
        }
        InternetDomainName suffix = null;
        // It appears the IDN class does not know about the various UK
        // second-level domains.
        // If it's a UK host, override the result by assuming three levels:
        if( host.endsWith(".uk")) {
            ImmutableList<String> parts = domainName.parts();
            if( parts.size() >= 3 ) {
                suffix = InternetDomainName.from(parts.get(parts.size() - 3)
                        + "." + parts.get(parts.size() - 2) + "."
                        + parts.get(parts.size() - 1));
            }
        } else {
            if( domainName.isTopPrivateDomain() || domainName.isUnderPublicSuffix() ) {
                suffix = domainName.topPrivateDomain();
            } else {
                suffix = domainName;
            }
        }

        // If it all failed for some reason, fall back on the host value:
        if (suffix == null)
            suffix = domainName;

        return suffix.toString();
    }
    
    /**
     * Returns a list of each level of the given host address. E.g. 'bbc.co.uk' would return:
     * [uk],[co.uk],[bbc.co.uk]
     *
     * @param host The full host address
     * @return An ImmutableList of Strings, one element per host level
     */
    public static ImmutableList<String> allLevels(String host) {
        // Default to empty list
        Builder<String> result = ImmutableList.builder();

        try {
            InternetDomainName domainName = InternetDomainName.from(host);
            result = parentLevels(domainName);
        }
        catch(NullPointerException e) {
            // ignore errors of this nature
        }
        catch (IllegalArgumentException e) {
            // This happens for IP-based hosts, see
            // https://github.com/ukwa/webarchive-discovery/issues/90
        }

        return result.build();
    }

    private static ImmutableList.Builder<String> parentLevels(InternetDomainName internetDomainName) {
        ImmutableList.Builder<String> levels;

        if(internetDomainName.hasParent()){
            levels = parentLevels(internetDomainName.parent());
        }
        else {
            levels = ImmutableList.builder();
        }

        levels.add(internetDomainName.toString());
        return levels;
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
