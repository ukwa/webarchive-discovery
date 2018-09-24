/**
 * 
 */
package uk.bl.wa.analyser.text;

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
    private List<AbstractTextAnalyser> analysers;

    public TextAnalysers(Config conf) {
        // Look up text analysers:
        analysers = AbstractTextAnalyser.getTextAnalysers(conf);
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
