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

import uk.bl.wa.solr.SolrRecord;

/**
 * @author anj
 *
 */
public abstract class TextAnalyser {

	/**
	 * Set up default analyser chain for text.
	 */
	static List<TextAnalyser> analysers = new ArrayList<TextAnalyser>();
	static {
		analysers.add( new LanguageAnalyser() );
		analysers.add( new PostcodeAnalyser() );
		analysers.add( new FuzzyHashAnalyser() );
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
	public static void runAllAnalysers( String text, SolrRecord solr ) {
		for( TextAnalyser ta : analysers ) {
			ta.analyse(text, solr);
		}
	}
	
	/**
	 * Sub-classes should implement this method to create text payload annotations for solr.
	 * 
	 * @param text
	 * @param solr
	 */
	public abstract void analyse( String text, SolrRecord solr );
	
}