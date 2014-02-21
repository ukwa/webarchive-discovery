/**
 * 
 */
package uk.bl.wa.parsers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
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
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.StackedNamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class StanfordAnnotatorParser extends AbstractParser {

	/** */
	private static final long serialVersionUID = 7995695364077961609L;

	/** */
	private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.unmodifiableSet(new HashSet<MediaType>(Arrays.asList(
                  MediaType.TEXT_PLAIN
            )));

	public static final Property NER_NAMES = Property.internalTextBag("NLP-NER-NAMES");

	/** */
	static Properties props = new Properties();
	static {
		//props.put("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		props.put("annotators", "tokenize, ssplit, pos, lemma, ner");
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
	    for(CoreMap sentence: sentences) {
	      // traversing the words in the current sentence
	      // a CoreLabel is a CoreMap with additional token-specific methods
	      for (CoreLabel token: sentence.get(TokensAnnotation.class)) {
	        // this is the text of the token
	        String word = token.get(TextAnnotation.class);
	        // this is the POS tag of the token
	        String pos = token.get(PartOfSpeechAnnotation.class);
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
		        	System.out.println("NE: "+currentEntityType+" "+currentEntity);
	        	} else {
	        		currentEntity += " " + token;
	        	}
	        }
	      }
	    }
	    
	}

	/**
	 * @param args
	 * @throws TikaException 
	 * @throws SAXException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException, SAXException, TikaException {
		String text = "The Autobiography of Malcolm X was published in 1965, the result of a collaboration between Malcolm X and journalist Alex Haley. Haley, Bob and Phillip Larkin coauthored the autobiography based on a series of in-depth interviews he conducted between 1963 and Malcolm X's 1965 assassination. The Autobiography is a spiritual conversion narrative that outlines Malcolm X's philosophy of black pride, black nationalism, and pan-Africanism. After the death of his subject, Haley authored the book's epilogue,a[›] which describes their collaboration and summarizes the end of Malcolm X's life. While Malcolm X and scholars contemporary to the book's publication regarded Haley as the book's ghostwriter, modern scholars tend to regard him as an essential collaborator who intentionally muted his authorial voice to allow readers to feel as though Malcolm X were speaking directly to them. Haley also influenced some of Malcolm X's literary choices; for example, when Malcolm X left the Nation of Islam during the composition of the book, Haley persuaded him to favor a style of \"suspense and drama\" rather than rewriting earlier chapters into a polemic against the Nation. Furthermore, Haley's proactive censorship of the manuscript's antisemitic material significantly influenced the ideological tone of the Autobiography, increasing its commercial success and popularity although distorting Malcolm X's public persona. When the Autobiography was published, the New York Times reviewer described it as a \"brilliant, painful, important book\". In 1967, historian John William Ward wrote that it would become a classic American autobiography. In 1998, Time named The Autobiography of Malcolm X one of ten \"required reading\" nonfiction books. A screenplay adaptation of the Autobiography by James Baldwin and Arnold Perl provided the source material for Spike Lee's 1992 film Malcolm X.";
		StanfordAnnotatorParser parser = new StanfordAnnotatorParser();
		Metadata metadata = new Metadata();
		parser.parse(new ByteArrayInputStream(text.getBytes()), null, metadata, null);
		System.out.println("RESULT: " + metadata );
	}
	
}
