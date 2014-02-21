/**
 * 
 */
package uk.bl.wa.parsers;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.tika.metadata.Metadata;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class StanfordAnnotatorParserTest {

	private StanfordAnnotatorParser parser;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		parser = new StanfordAnnotatorParser();
	}

	@Test
	public void testNERMalcolmX() {
		String text = "The Autobiography of Malcolm X was published in 1965, the result of a collaboration between Malcolm X and journalist Alex Haley. Haley coauthored the autobiography based on a series of in-depth interviews he conducted between 1963 and Malcolm X's 1965 assassination. The Autobiography is a spiritual conversion narrative that outlines Malcolm X's philosophy of black pride, black nationalism, and pan-Africanism. After the death of his subject, Haley authored the book's epilogue,a[›] which describes their collaboration and summarizes the end of Malcolm X's life. While Malcolm X and scholars contemporary to the book's publication regarded Haley as the book's ghostwriter, modern scholars tend to regard him as an essential collaborator who intentionally muted his authorial voice to allow readers to feel as though Malcolm X were speaking directly to them. Haley also influenced some of Malcolm X's literary choices; for example, when Malcolm X left the Nation of Islam during the composition of the book, Haley persuaded him to favor a style of \"suspense and drama\" rather than rewriting earlier chapters into a polemic against the Nation. Furthermore, Haley's proactive censorship of the manuscript's antisemitic material significantly influenced the ideological tone of the Autobiography, increasing its commercial success and popularity although distorting Malcolm X's public persona. When the Autobiography was published, the New York Times reviewer described it as a \"brilliant, painful, important book\". In 1967, historian John William Ward wrote that it would become a classic American autobiography. In 1998, Time named The Autobiography of Malcolm X one of ten \"required reading\" nonfiction books. A screenplay adaptation of the Autobiography by James Baldwin and Arnold Perl provided the source material for Spike Lee's 1992 film Malcolm X.";
		
		// Launch the parser:
		Metadata metadata = new Metadata();
		parser.parse(text, metadata);
		
		Set<String> persons = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_PERSONS)));
		System.out.println("PERSONS: "+persons);
		assertTrue(persons.contains("Malcolm X"));
		
		Set<String> orgs = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_ORGANISATIONS)));
		System.out.println("ORGANIZATIONS: "+orgs);
		assertTrue(orgs.contains("New York Times"));
		
		Set<String> locs = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_LOCATIONS)));
		System.out.println("LOCATIONS: "+locs);
		
		Set<String> dates = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_DATES)));
		System.out.println("DATES: "+dates);
		assertTrue(dates.contains("1965"));
		
		Set<String> misc = new HashSet<String>(Arrays.asList(metadata.getValues(StanfordAnnotatorParser.NER_MISC)));
		System.out.println("MISC: "+misc);
		assertTrue(misc.contains("American"));
	}

}
