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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreLabel.OutputFormat;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * 
 * Note that by running the test data though the sentiment scorer, we infer that the integer sentiment score maps as:
 * 
 * 0 = −−
 * 1 = −
 * 2 = 0
 * 3 = +
 * 4 = + +
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class StanfordAnnotatorParser extends AbstractParser {

    /** */
    private static final long serialVersionUID = 7995695364077961609L;

    /** Supported types - plain text only */
    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.unmodifiableSet(new HashSet<MediaType>(Arrays.asList(
                    MediaType.TEXT_PLAIN
                    )));
    
    /* Named Entity Types */
    public static final Property NER_PERSONS = Property.internalTextBag("NLP-NER-PERSONS");
    public static final Property NER_ORGANISATIONS = Property.internalTextBag("NLP-NER-ORGANISATIONS");
    public static final Property NER_LOCATIONS = Property.internalTextBag("NLP-NER-LOCATIONS");
    public static final Property NER_DATES = Property.internalTextBag("NLP-NER-DATES");
    public static final Property NER_MISC = Property.internalTextBag("NLP-NER-MISC");
    
    /** Sentiment average score, rounded to nearest integer on the 0-4 scale */
    public static final Property AVG_SENTIMENT = Property.internalInteger("AVG-SENTIMENT");
    public static final Property SENTIMENT_DIST = Property.internalIntegerSequence("SENTENCE-DIST");

    /** */
    static Properties props = new Properties();
    static {
        //props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
        props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, sentiment");
    }

    /** */
    static StanfordCoreNLP pipeline = new StanfordCoreNLP(props);

    /**
     * 
     */
    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    /**
     * 
     */
    @Override
    public void parse(InputStream stream, ContentHandler handler,
            Metadata metadata, ParseContext context) throws IOException,
            SAXException, TikaException {

        // Read the input stream as text:
        StringWriter writer = new StringWriter();
        IOUtils.copy(stream, writer, "UTF-8");
        String text = writer.toString();

        // And parse:
        this.parse(text, metadata);
    }

    /**
     * 
     * @param text
     * @param metadata
     */
    public void parse(String text, Metadata metadata ) {

        // create an empty Annotation just with the given text
        Annotation document = new Annotation(text);

        // run all Annotators on this text
        pipeline.annotate(document);

        // these are all the sentences in this document
        // a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = document.get(SentencesAnnotation.class);

        // Loop over and extract:
        boolean inEntity = false;
        String currentEntity = "";
        String currentEntityType = "";
        HashSet<String> persons = new HashSet<String>();
        HashSet<String> orgs = new HashSet<String>();
        HashSet<String> locations = new HashSet<String>();
        HashSet<String> dates = new HashSet<String>();
        HashSet<String> miscs = new HashSet<String>();
        double totalSentiment = 0;
        double totalSentences = 0;
        int[] sentiments = new int[5];
        for(CoreMap sentence: sentences) {
            // REQUIRES LATER VERSION OF PARSER (Java 8)
            // Tree tree = sentence.get(SentimentAnnotatedTree.class);
            // int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
            // totalSentiment += sentiment;
            // totalSentences++;
            // // Also store as a histogram:
            // sentiments[sentiment]++;
            
            // traversing the words in the current sentence
            // a CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
                // this is the text of the token
                //String word = token.get(TextAnnotation.class);
                // this is the POS tag of the token
                //String pos = token.get(PartOfSpeechAnnotation.class);
                // this is the NER label of the token
                String ne = token.get(NamedEntityTagAnnotation.class);
                if( ! inEntity ) {
                    if( ! "O".equals(ne) ) {
                        inEntity = true;
                        currentEntity = "";
                        currentEntityType = ne;
                    }
                }
                if( inEntity ) {
                    if( "O".equals(ne) ) {
                        inEntity = false;
                        if( "PERSON".equals(currentEntityType)) {
                            persons.add(currentEntity.trim());
                        } else if( "ORGANIZATION".equals(currentEntityType)) {
                            orgs.add(currentEntity.trim());
                        } else if( "LOCATION".equals(currentEntityType)) {
                            locations.add(currentEntity.trim());
                        } else if( "DATE".equals(currentEntityType)) {
                            dates.add(currentEntity.trim());
                        } else if( "MISC".equals(currentEntityType)) {
                            miscs.add(currentEntity.trim());
                        } else if( "NUMBER".equals(currentEntityType)) {
                            // Ignore numbers.
                        } else {
                            System.err.println("Entity type "+currentEntityType+" for token "+token+" cannot be handled by this parser!");
                        }
                    } else {
                        currentEntity += " "
                                + token.toString(OutputFormat.VALUE);
                    }
                }
            }
        }

        // Now store them:
        metadata.set( NER_PERSONS, persons.toArray(new String[0]) );
        metadata.set( NER_ORGANISATIONS, orgs.toArray(new String[0]) );
        metadata.set( NER_LOCATIONS, locations.toArray(new String[0]) );
        metadata.set( NER_DATES, dates.toArray(new String[0]) );
        metadata.set( NER_MISC, miscs.toArray(new String[0]) );
        // And calculate and store the rounded average sentiment:
        metadata.set( AVG_SENTIMENT, (int)Math.round(totalSentiment/totalSentences) );
        // Convert sentiment distribution:
        // String[] sentiment_dist = new String[5];
        // for( int i = 0; i < 5; i++ ) sentiment_dist[i] = ""+sentiments[i];
        // metadata.set( SENTIMENT_DIST, sentiment_dist);
    }

    /**
     * @param args
     * @throws TikaException 
     * @throws SAXException 
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException, SAXException, TikaException {
        String text = "This movie doesn't care about cleverness, wit or any other kind of intelligent humor. Those who find ugly meanings in beautiful things are corrupt without being charming. There are slow and repetitive parts, but it has just enough spice to keep it interesting.";
        StanfordAnnotatorParser parser = new StanfordAnnotatorParser();
        Metadata metadata = new Metadata();
        parser.parse(new ByteArrayInputStream(text.getBytes()), null, metadata, null);
        System.out.println("SENTIMENT: " + metadata.get(AVG_SENTIMENT) );
        System.out.println("# PERSONS: " + metadata.getValues(NER_PERSONS).length );
        System.out.println("# ORGANISATIONS: " + metadata.getValues(NER_ORGANISATIONS).length );
    }

}
