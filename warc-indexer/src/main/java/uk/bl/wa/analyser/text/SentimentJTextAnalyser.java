/**
 * 
 */
package uk.bl.wa.analyser.text;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.typesafe.config.Config;

import uk.bl.wa.sentimentalj.Sentiment;
import uk.bl.wa.sentimentalj.SentimentalJ;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author anj
 *
 */
public class SentimentJTextAnalyser extends AbstractTextAnalyser {
    private static Logger log = LoggerFactory.getLogger(SentimentJTextAnalyser.class );

    /** */
    private static SentimentalJ sentij = new SentimentalJ();

    /**
     * @param conf
     */
    public void configure(Config conf) {
        if (conf.hasPath("warc.index.extract.content.text_sentimentj") && conf
                .getBoolean("warc.index.extract.content.text_sentimentj")) {
            setEnabled(true);
        } else {
            setEnabled(false);
        }
    }

    /**
     * 
     */
    public void analyse( String text, SolrRecord solr ) {
        // Sentiment Analysis:
        int sentilen = 10000;
        if( sentilen > text.length() )
            sentilen = text.length();
        String sentitext = text.substring( 0, sentilen );
        // metadata.get(HtmlFeatureParser.FIRST_PARAGRAPH);

        Sentiment senti = sentij.analyze( sentitext );
        double sentilog = Math.signum( senti.getComparative() ) * ( Math.log( 1.0 + Math.abs( senti.getComparative() ) ) / 40.0 );
        int sentii = ( int ) ( SolrFields.SENTIMENTS.length * ( 0.5 + sentilog ) );
        if( sentii < 0 ) {
            log.debug( "Caught a sentiment rating less than zero: " + sentii + " from " + sentilog );
            sentii = 0;
        }
        if( sentii >= SolrFields.SENTIMENTS.length ) {
            log.debug( "Caught a sentiment rating too large to be in range: " + sentii + " from " + sentilog );
            sentii = SolrFields.SENTIMENTS.length - 1;
        }
        // if( sentii != 3 )
        // log.debug("Got sentiment: " + sentii+" "+sentilog+" "+ SolrFields.SENTIMENTS[sentii] );
        // Map to sentiment scale:
        solr.addField( SolrFields.SENTIMENT, SolrFields.SENTIMENTS[ sentii ] );
        solr.addField( SolrFields.SENTIMENT_SCORE, "" + senti.getComparative() );
    }
}
