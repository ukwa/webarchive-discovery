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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.metadata.Metadata;

import uk.bl.wa.parsers.StanfordAnnotatorParser;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author anj
 *
 */
public class StanfordAnalyser extends TextAnalyser {

	StanfordAnnotatorParser parser = new StanfordAnnotatorParser();
	
	public static final int MAX_CHARS_TO_ANALYSE = 10000;

	/* (non-Javadoc)
	 * @see uk.bl.wa.analyser.text.TextAnalyser#analyse(java.lang.String, uk.bl.wa.util.solr.SolrRecord)
	 */
	@Override
	public void analyse(String text, SolrRecord solr) {
		int sentilen = MAX_CHARS_TO_ANALYSE;
		if( sentilen > text.length() )
			sentilen = text.length();
		String sentitext = text.substring( 0, sentilen );
		
		Metadata metadata = new Metadata();
		parser.parse(sentitext, metadata);
		
		Set<String> persons = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_PERSONS)));
		System.out.println("PERSONS: "+persons);
		
		Set<String> orgs = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_ORGANISATIONS)));
		System.out.println("ORGANIZATIONS: "+orgs);
		
		Set<String> locs = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_LOCATIONS)));
		System.out.println("LOCATIONS: "+locs);
		
		Set<String> dates = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_DATES)));
		System.out.println("DATES: "+dates);
		
		Set<String> misc = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_MISC)));
		System.out.println("MISC: "+misc);

		/* And sentiments */
		
		String sentiment = metadata.get(StanfordAnnotatorParser.AVG_SENTIMENT);
		System.out.println("Sentiment: "+sentiment);
		solr.addField( SolrFields.SENTIMENT, sentiment );

		List<String> sentiments = Arrays.asList(metadata.getValues(StanfordAnnotatorParser.SENTIMENT_DIST));
		System.out.println("Sentiments: "+sentiments);
	}

}
