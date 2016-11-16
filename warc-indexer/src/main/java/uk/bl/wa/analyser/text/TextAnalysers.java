/**
 * 
 */
package uk.bl.wa.analyser.text;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2014 The UK Web Archive
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

import java.util.ArrayList;
import java.util.List;

import com.typesafe.config.Config;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;

/**
 * @author anj
 *
 */
public class TextAnalysers {

	/**
	 * Set up analyser chain for text.
	 */
	private List<AbstractTextAnalyser> analysers = new ArrayList<AbstractTextAnalyser>();

	public TextAnalysers(Config conf) {

		// Add the language analyser:
		analysers.add( new LanguageAnalyser(conf) );

		// Add the postcode extractor:
		if (!conf.hasPath("warc.index.extract.content.text_extract_postcodes")
				|| conf.getBoolean("warc.index.extract.content.text_extract_postcodes"))
		analysers.add( new PostcodeAnalyser(conf) );

		// Add the ssdeep hasher:
		if (!conf.hasPath("warc.index.extract.content.text_fuzzy_hash")
				|| conf.getBoolean("warc.index.extract.content.text_fuzzy_hash"))
			analysers.add(new FuzzyHashAnalyser(conf));

		// NLP analysers:

		// This runs ok, but does not give very good results:
		if (conf.hasPath("warc.index.extract.content.text_sentimentj")
				&& conf.getBoolean("warc.index.extract.content.text_sentimentj")) {
			analysers.add(new SentimentJTextAnalyser(conf));
		}

		// This runs very slowly, but should give good results:
		if (conf.hasPath("warc.index.extract.content.text_stanford_ner")
				&& conf.getBoolean("warc.index.extract.content.text_stanford_ner")) {
            throw new RuntimeException(
                    "Stanford Analyser not currently supported!");
            // analysers.add(new StanfordAnalyser(conf));
		}

        // TODO Add GATE to the nlp package
		/*
		 * if (conf.hasPath("warc.index.extract.content.text_gate") &&
		 * conf.getBoolean("warc.index.extract.content.text_gate")) {
		 * analysers.add(new GateTextAnalyser(conf)); }
		 */
	}
	
	/**
	 * Run all configured analysers on the text.
	 * 
	 * @param text
	 * @param solr
	 */
	public void analyse( SolrRecord solr ) {
        final long start = System.nanoTime();
		// Pull out the text:
		if( solr.getField( SolrFields.SOLR_EXTRACTED_TEXT ) != null ) {
			String text = ( String ) solr.getField( SolrFields.SOLR_EXTRACTED_TEXT ).getFirstValue();
			text = text.trim();
			if( !"".equals( text ) ) {
				for( AbstractTextAnalyser ta : analysers ) {
					ta.analyse(text, solr);
				}
			}
		}
        Instrument.timeRel("WARCIndexer.extract#total", "TextAnalyzers#total", start);
	}
	
}