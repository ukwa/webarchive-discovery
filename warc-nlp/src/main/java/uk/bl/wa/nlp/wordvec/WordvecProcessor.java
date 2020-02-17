package uk.bl.wa.nlp.wordvec;

/*-
 * #%L
 * warc-nlp
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

import java.io.FileReader;
import java.util.Collection;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordvecProcessor {

    private static Logger log = LoggerFactory.getLogger(WordvecProcessor.class);

    public static void main(String[] args) throws Exception {

        SentenceIterator iter = new StanfordSentenceIterator(
                new FileReader("src/test/resources/Mona_Lisa.txt"));

        // Use Stanford NLP sentence splitter:

        // Split on white spaces in the line to get words
        TokenizerFactory t = new DefaultTokenizerFactory();
        t.setTokenPreProcessor(new CommonPreprocessor());

        log.info("Building model....");
        Word2Vec vec = new Word2Vec.Builder().minWordFrequency(5).iterations(1)
                .layerSize(100).seed(42).windowSize(5).iterate(iter)
                .tokenizerFactory(t).build();

        log.info("Fitting Word2Vec model....");
        vec.fit();

        log.info("Writing word vectors to text file....");

        // Write word vectors
        WordVectorSerializer.writeWordVectors(vec, "pathToWriteto.txt");
        WordVectorSerializer.writeFullModel(vec, "pathToWriteto.model");

        log.info("Closest Words:");
        Collection<String> lst = vec.wordsNearest("french", 10);
        System.out.println(lst);
        // UiServer server = UiServer.getInstance();
        // System.out.println("Started on port " + server.getPort());


    }
}
