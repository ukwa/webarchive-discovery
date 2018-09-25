/**
 * 
 */
package uk.bl.wa.analyser.text;

import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.Test;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrRecordFactory;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Oscar4TextAnalyserTest {

    @Test
    public void test() {
        String test = "Hello acetone world!";

        Oscar4TextAnalyser oa = new Oscar4TextAnalyser();
        SolrRecord solr = SolrRecordFactory.createFactory(null).createRecord();

        oa.analyse(test, solr);

        Collection<Object> results = solr
                .getField(SolrFields.SOLR_TIKA_METADATA_LIST)
                .getValues();

        assertTrue("Cannot find expected value.", results
                .contains("OSCAR4:MATCH:acetone"));

        assertTrue("Cannot find expected value.", results
                .contains("OSCAR4:STD_INCHI:InChI=1S/C3H6O/c1-3(2)4/h1-2H3"));

    }

}
