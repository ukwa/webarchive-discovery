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

import com.typesafe.config.Config;

import uk.bl.wa.extract.LanguageDetector;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author anj
 *
 */
public class LanguageAnalyser extends AbstractTextAnalyser {
	
	/** */
	private LanguageDetector ld = new LanguageDetector();

	/**
	 * @param conf
	 */
	public LanguageAnalyser(Config conf) {
	}

	/* (non-Javadoc)
	 * @see uk.bl.wa.analyser.text.TextAnalyser#analyse(java.lang.String, uk.bl.wa.util.solr.SolrRecord)
	 */
	@Override
	public void analyse(String text, SolrRecord solr) {
		String li = ld.detectLanguage( text );
		if( li != null )
			solr.addField( SolrFields.CONTENT_LANGUAGE, li );
	}

}
