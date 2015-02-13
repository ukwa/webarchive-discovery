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

	protected TextAnalysers() {
	}

	public TextAnalysers( Config conf ) {
		
		analysers.add( new LanguageAnalyser(conf) );
		analysers.add( new PostcodeAnalyser(conf) );
		analysers.add( new FuzzyHashAnalyser(conf) );
		// This runs ok, but does not give very good results:
		//analysers.add( new SentimentJTextAnalyser() );
		// This runs very slowly, but should give good results:
		//analysers.add( new StanfordAnalyser() );
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