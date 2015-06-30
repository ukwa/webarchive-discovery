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
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uk.bl.wa.analyser.text.lang.LanguageIdentifier;

import com.cybozu.labs.langdetect.DetectorFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class LanguageDetectorTest extends TestCase {

    public void testLangdetectConfig() {
        DetectorFactory.clear();
        testLangdetectConfig("arcnameanalyser.conf", 8, 2);
        DetectorFactory.clear();
        testLangdetectConfig("reference.conf", LanguageDetector.DEFAULT_LANGDETECT_PROFILES.length, 2);
    }

    private void testLangdetectConfig(String config, int expectedLangdetect, int expectedTika) {
        Config conf = ConfigFactory.parseURL(Thread.currentThread().getContextClassLoader().getResource(config));
        new LanguageDetector(conf);

        assertEquals("The number of langdetect language profiles after initialization should match config " + config,
                     expectedLangdetect, DetectorFactory.getLangList().size());
        // Defined in tika.language.override.properties
        assertEquals("The number of Tika LanguageIdentifier profiles after initialization should be as expected when "
                     + "using config " + config,
                     expectedTika, LanguageIdentifier.getSupportedLanguages().size());
    }
}
