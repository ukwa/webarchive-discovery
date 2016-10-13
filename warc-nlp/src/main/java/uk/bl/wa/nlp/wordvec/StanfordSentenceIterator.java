/**
 * 
 */
package uk.bl.wa.nlp.wordvec;

import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;
import java.util.List;

import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.SentencePreProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.Sentence;
import edu.stanford.nlp.process.DocumentPreprocessor;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class StanfordSentenceIterator implements SentenceIterator {

    private static Logger log = LoggerFactory
            .getLogger(StanfordSentenceIterator.class);

    private DocumentPreprocessor dp;
    private Iterator<List<HasWord>> dpi;

    public StanfordSentenceIterator(String paragraph) {
        Reader reader = new StringReader(paragraph);
        dp = new DocumentPreprocessor(reader);
        dpi = dp.iterator();
    }

    public StanfordSentenceIterator(Reader reader) {
        dp = new DocumentPreprocessor(reader);
        dpi = dp.iterator();
    }

    /* (non-Javadoc)
     * @see org.deeplearning4j.text.sentenceiterator.SentenceIterator#nextSentence()
     */
    @Override
    public String nextSentence() {
        List<HasWord> item = dpi.next();
        String sentence = Sentence.listToString(item);
        log.info("Got item " + sentence);
        return sentence;
    }

    /* (non-Javadoc)
     * @see org.deeplearning4j.text.sentenceiterator.SentenceIterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        try {
            return dpi.hasNext();
        } catch (Exception e) {
            log.error("Exception when looking for the next sentence!", e);
            return false;
        }
    }

    /* (non-Javadoc)
     * @see org.deeplearning4j.text.sentenceiterator.SentenceIterator#reset()
     */
    @Override
    public void reset() {
        dpi = dp.iterator();
    }

    /* (non-Javadoc)
     * @see org.deeplearning4j.text.sentenceiterator.SentenceIterator#finish()
     */
    @Override
    public void finish() {
    }

    /* (non-Javadoc)
     * @see org.deeplearning4j.text.sentenceiterator.SentenceIterator#getPreProcessor()
     */
    @Override
    public SentencePreProcessor getPreProcessor() {
        return null;
    }

    /* (non-Javadoc)
     * @see org.deeplearning4j.text.sentenceiterator.SentenceIterator#setPreProcessor(org.deeplearning4j.text.sentenceiterator.SentencePreProcessor)
     */
    @Override
    public void setPreProcessor(SentencePreProcessor preProcessor) {
        throw new RuntimeException("Unsupported");
    }

}
