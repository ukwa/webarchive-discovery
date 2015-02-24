package uk.bl.wa.extract;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2015 State and University Library, Denmark
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
import com.cybozu.labs.langdetect.DetectorFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import junit.framework.TestCase;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.archive.io.ArchiveRecordHeader;
import uk.bl.wa.analyser.text.lang.LanguageIdentifier;
import uk.bl.wa.solr.SolrRecord;

import java.util.Map;
import java.util.Set;

public class LanguageDetectorTest extends TestCase {
    private static Log log = LogFactory.getLog(LanguageDetectorTest.class);

    public void testLangdetectConfig() {
        Config conf = ConfigFactory.parseURL(
                Thread.currentThread().getContextClassLoader().getResource("arcnameanalyser.conf"));
        new LanguageDetector(conf);
        assertEquals("The number of langdetect language profiles after initialization should match config",
                     conf.getStringList("warc.index.extract.content.language.langdetectprofiles").size(),
                     DetectorFactory.getLangList().size());
        // Defined in tika.language.override.properties
        assertEquals("The number of Tika LanguageIdentifier profiles after initialization should be as expected",
                     2, LanguageIdentifier.getSupportedLanguages().size());
    }
}
