/**
 * 
 */
package uk.bl.wa.nlp.parsers;

/*
 * #%L
 * warc-indexer
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.metadata.Metadata;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 *         Temporarily setting this to Ignore for testing as it consumes too
 *         much RAM and causes Travis to fail.
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

        /* And sentiments */
        // Requires Java 8
        
        // String sentiment =
        // metadata.get(StanfordAnnotatorParser.AVG_SENTIMENT);
        // System.out.println("Sentiment: "+sentiment);
        //
        // List<String> sentiments =
        // Arrays.asList(metadata.getValues(StanfordAnnotatorParser.SENTIMENT_DIST));
        // System.out.println("Sentiments: "+sentiments);
    }

    // Requires Java 8
    // Test
    @Ignore
    public void testNERSentiment() {
        String text = "This movie doesn't care about cleverness, wit or any other kind of intelligent humor. Those who find ugly meanings in beautiful things are corrupt without being charming. There are slow and repetitive parts, but it has just enough spice to keep it interesting.";
        Metadata metadata = new Metadata();
        parser.parse(text, metadata);
        String sentiment = metadata.get(StanfordAnnotatorParser.AVG_SENTIMENT);
        System.out.println("Sentiment: "+sentiment);
        assertTrue(sentiment.equals("2"));
        
        List<String> sentiments = Arrays.asList(metadata.getValues(StanfordAnnotatorParser.SENTIMENT_DIST));
        System.out.println("Sentiments: "+sentiments);
        
    }
}
