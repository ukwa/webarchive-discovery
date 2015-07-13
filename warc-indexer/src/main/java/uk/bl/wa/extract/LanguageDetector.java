/**
 * 
 */
package uk.bl.wa.extract;

/*
 * #%L
 * warc-indexer
 * $Id:$
 * $HeadURL:$
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import uk.bl.wa.analyser.text.lang.LanguageIdentifier;
import uk.bl.wa.util.Instrument;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import com.typesafe.config.Config;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class LanguageDetector {
	private static Log log = LogFactory.getLog(LanguageDetector.class);
	
	public static final String[] DEFAULT_LANGDETECT_PROFILES = new String[] {
            "af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "et", "fa", "fi", "fr", "gu",
            "he", "hi", "hr", "hu", "id", "it", "ja", "kn", "ko", "lt", "lv", "mk", "ml", "mr",
            "ne", "nl", "no", "pa", "pl", "pt", "ro", "ru", "sk", "sl", "so", "sq", "sv", "sw",
            "ta", "te", "th", "tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw"
	};

	// Only take the first 100,000 characters:
	private static final int MAX_TEXT_LEN = 100 * 1000;

    public LanguageDetector(Config conf) {
        final long start = System.nanoTime();
        if (conf != null && conf.hasPath("warc.index.extract.content.language.langdetectprofiles")) {
            init(conf.getStringList("warc.index.extract.content.language.langdetectprofiles"));
			log.info("Initialising LanguageDetector using app configuration.");
        } else {
            init(Arrays.asList(DEFAULT_LANGDETECT_PROFILES));
			log.info("Initialising LanguageDetector using library defaults.");
        }
        Instrument.timeRel("LanguageAnalyzer#total", "LanguageDetector#startup", start);
    }

	public void init(List<String> langdetectProfiles) {
		// Set up the langdetect:
		if( DetectorFactory.getLangList().size() != 0 ) {
            log.info("langdetect already initialized with " + DetectorFactory.getLangList().size() + " profiles");
            return;
        }
		List<String> json_profiles = new ArrayList<String>();
		for( String lc : langdetectProfiles ) {
			BufferedReader is = new BufferedReader( new InputStreamReader(
					this.getClass().getResourceAsStream("/lang-detect-profiles/"+lc)
					) );
			StringBuilder sb = new StringBuilder();
			String line;
			try {
				while ((line = is.readLine()) != null) {
					sb.append(line+"\n");
				}
				json_profiles.add(sb.toString());
			} catch( IOException e ) {
				log.error("Exception while reading langdetect profile: "+lc);
			}

		}
		try {
			DetectorFactory.loadProfile(json_profiles);
            log.info("Initialized with " + langdetectProfiles.size() + " langdetect profiles and "
                     + LanguageIdentifier.getSupportedLanguages().size() + " Tika LanguageIdentifier profiles");
		} catch( LangDetectException e) {
			log.error("Error occurred when loading language profiles:"+e+" Language detection will likely fail. ");
		}
	}

    /**
	 * Detect using the langdetect method.
	 * 
	 * @param text
	 * @return language code
	 */
	private String getLangdetectLanguage( String text ) {
		try {
			Detector detector = DetectorFactory.create();
			detector.append(text);
			return detector.detect();
		} catch (LangDetectException e) {
			log.info("Could not detect language: "+e);
			return null;
        }
	}
	
	public String detectLanguage( String text ) {
        final long start = System.nanoTime();
        try {
			// Avoid ngramming totally massive texts:
			if (text != null && text.length() > MAX_TEXT_LEN) {
				text = text.substring(0, MAX_TEXT_LEN);
			}
            // Attept to use Tika module first (should be good for long texts)
            LanguageIdentifier li = new LanguageIdentifier(text);
            // Only return that result if it's credible:
            if( li.isReasonablyCertain() ) return li.getLanguage();
        } finally {
            Instrument.timeRel("LanguageAnalyzer#total", "LanguageDetector.detectLanguage#li", start);
        }
        final long startLD = System.nanoTime();
        try {
            // Otherwise, fall back to the langdetect approach (should be better for short texts)
            // (Having found that (very) short texts are not likely to be classified sensibly.)
            if( text.length() > 200 )
                return this.getLangdetectLanguage(text);
            return null;
        } finally {
            Instrument.timeRel("LanguageAnalyzer#total", "LanguageDetector.detectLanguage#ld", startLD);
        }
	}
	/**
	 * @param args
	 * @throws LangDetectException 
	 */
	public static void main(String[] args) throws LangDetectException {
        LanguageDetector ld = new LanguageDetector(null);
        System.out.println("Lang: " + ld.detectLanguage("I just wanted to double check that the IP address we need to redirect these to is the IP address of www.webarchive.org.uk.  which is"));
	}

}
