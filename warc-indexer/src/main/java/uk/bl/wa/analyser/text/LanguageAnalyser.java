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
import org.apache.tika.langdetect.OptimaizeLangDetector;
import org.apache.tika.language.detect.LanguageDetector;
import org.apache.tika.language.detect.LanguageResult;

import com.typesafe.config.Config;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.util.Instrument;

/**
 * @author anj
 *
 */
public class LanguageAnalyser extends AbstractTextAnalyser {
    private Logger log = LoggerFactory.getLogger(LanguageAnalyser.class);
    
    /** */
    private LanguageDetector ld;

    /**
     * @param conf
     */
    public void configure(Config conf) {
        setEnabled(!conf.hasPath("warc.index.extract.content.language.enabled")
                || conf.getBoolean(
                        "warc.index.extract.content.language.enabled"));
        ld = new OptimaizeLangDetector().loadModels();
        log.info(
                "Constructed language analyzer with enabled = " + isEnabled());
    }

    /* (non-Javadoc)
     * @see uk.bl.wa.analyser.text.TextAnalyser#analyse(java.lang.String, uk.bl.wa.util.solr.SolrRecord)
     */
    @Override
    public void analyse(String text, SolrRecord solr) {
        final long start = System.nanoTime();
        try {
            LanguageResult li = ld.detect(text);
            if (li != null) {
                solr.addField(SolrFields.CONTENT_LANGUAGE, li.getLanguage());
            }
        } catch (IllegalArgumentException e) {
            log.error("Exception when determining language of this item: "
                    + e.getMessage(), e);
            solr.addParseException(e);
        }
        Instrument.timeRel("TextAnalyzers#total", "LanguageAnalyzer#total", start);
    }

}
