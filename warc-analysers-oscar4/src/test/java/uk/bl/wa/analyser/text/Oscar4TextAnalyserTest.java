/**
 * 
 */
package uk.bl.wa.analyser.text;

/*-
 * #%L
 * warc-analysers-oscar4
 * %%
 * Copyright (C) 2013 - 2020 The webarchive-discovery project contributors
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
