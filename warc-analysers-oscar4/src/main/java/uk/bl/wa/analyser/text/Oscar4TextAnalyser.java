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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.typesafe.config.Config;

import uk.ac.cam.ch.wwmm.oscar.Oscar;
import uk.ac.cam.ch.wwmm.oscar.chemnamedict.entities.ChemicalStructure;
import uk.ac.cam.ch.wwmm.oscar.chemnamedict.entities.FormatType;
import uk.ac.cam.ch.wwmm.oscar.chemnamedict.entities.ResolvedNamedEntity;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Oscar4TextAnalyser extends AbstractTextAnalyser {

    Oscar oscar = new Oscar();

    /* (non-Javadoc)
     * @see uk.bl.wa.analyser.text.AbstractTextAnalyser#configure(com.typesafe.config.Config)
     */
    @Override
    public void configure(Config conf) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see uk.bl.wa.analyser.text.AbstractTextAnalyser#analyse(java.lang.String, uk.bl.wa.solr.SolrRecord)
     */
    @Override
    public void analyse(String text, SolrRecord solr) {
        // Find the entities:
        List<ResolvedNamedEntity> entities = oscar
                .findAndResolveNamedEntities(text);
        // Record them:
        Set<String> uniqueEntities = new HashSet<String>();
        for (ResolvedNamedEntity ne : entities) {
            // e.g. 'acetone'
            uniqueEntities.add("OSCAR4:MATCH:" + ne.getSurface());
            ChemicalStructure stdInchi = ne
                    .getFirstChemicalStructure(FormatType.STD_INCHI);
            if (stdInchi != null) {
                // e.g. [Structure:STD_INCHI:InChI=1S/C3H6O/c1-3(2)4/h1-2H3]
                uniqueEntities.add("OSCAR4:" + stdInchi.getType() + ":"
                        + stdInchi.getValue());
            }
        }
        // Store in Solr records:
        for (String ent : uniqueEntities) {
            solr.addField(SolrFields.SOLR_TIKA_METADATA_LIST, ent);
        }
    }

}
