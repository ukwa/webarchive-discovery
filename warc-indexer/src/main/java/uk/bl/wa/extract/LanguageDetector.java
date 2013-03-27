/**
 * 
 */
package uk.bl.wa.extract;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.language.LanguageIdentifier;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class LanguageDetector {
	private static Log log = LogFactory.getLog(LanguageDetector.class);
	
	private static String[] langdetect_profiles = new String[] {
		"af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "et", "fa", "fi", "fr", "gu", "he", "hi", "hr", "hu", "id", "it", "ja", "kn", "ko", "lt", "lv", "mk", "ml", "mr", "ne", "nl", "no", "pa", "pl", "pt", "ro", "ru", "sk", "sl", "so", "sq", "sv", "sw", "ta", "te", "th", "tl", "tr", "uk", "ur", "vi", "zh-cn", "zh-tw"
	};

	public LanguageDetector() {
		// Set up the langdetect:
		if( DetectorFactory.getLangList().size() == 0 ) {
		List<String> json_profiles = new ArrayList<String>();
		for( String lc : langdetect_profiles ) {
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
		} catch( LangDetectException e) {
			log.error("Error occured when loading language profiles:"+e+" Language detection will likely fail. ");
		}
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
		// Attept to use Tika module first (should be good for long texts)
		LanguageIdentifier li = new LanguageIdentifier(text);
		// Only return that result if it's credible:
		if( li.isReasonablyCertain() ) return li.getLanguage();
		// Otherwise, fall back to the langdetect approach (should be better for short texts)
		return this.getLangdetectLanguage(text);
	}
	/**
	 * @param args
	 * @throws LangDetectException 
	 */
	public static void main(String[] args) throws LangDetectException {
		LanguageDetector ld = new LanguageDetector();
		System.out.println("Lang: " + ld.detectLanguage("I just wanted to double check that the IP address we need to redirect these to is the IP address of www.webarchive.org.uk.  which is"));

	}

}
