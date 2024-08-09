/**
 * 
 */
package uk.bl.wa.analyser.text;

/*-
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2024 The webarchive-discovery project contributors
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

import com.carrotsearch.labs.langid.DetectedLanguage;
import com.carrotsearch.labs.langid.LangIdV3;
import com.typesafe.config.Config;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;

/**
 * @author Toth
 *
 */
public class LanguageAnalyser extends AbstractTextAnalyser 
{
    private Logger log = LoggerFactory.getLogger(LanguageAnalyser.class);
    
    // The language detection model
    private LangIdV3 langid;

    /**
     * @param conf
     */
    public void configure(Config conf)
    {
        setEnabled(!conf.hasPath("warc.index.extract.content.language.enabled")
                || conf.getBoolean("warc.index.extract.content.language.enabled"));
        
        this.langid = new LangIdV3();
        
        log.debug("Constructed language analyzer with enabled = " + isEnabled());
    }

    @Override
    public void analyse(String text, SolrRecord solr) 
    {
        final long start = System.nanoTime();
        
        try
        {
        	DetectedLanguage result = langid.classify(text, true);
            
            if (result != null) 
            {
                solr.addField(SolrFields.CONTENT_LANGUAGE, result.getLangCode());
            }
        }
        catch (IllegalArgumentException e) 
        {
            log.error("Exception when determining language of this item: " + e.getMessage(), e);
            solr.addParseException(e);
        }
        
        Instrument.timeRel("TextAnalyzers#total", "LanguageAnalyzer#total", start);
    }

}
