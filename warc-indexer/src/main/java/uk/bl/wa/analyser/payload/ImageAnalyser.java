/**
 * 
 */
package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
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

import java.io.InputStream;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;

/**
 * @author anj
 *
 */
public class ImageAnalyser extends AbstractPayloadAnalyser {
    private static Logger log = LoggerFactory.getLogger( ImageAnalyser.class );

    /** Maximum file size of images to attempt to parse */
    private long max_size_bytes = 1000;

    /** Random sampling rate */
    private double sampleRate = 100;
    private long sampleCount = 0;

    private boolean extractImageFeatures = false;

    public ImageAnalyser() {
    }

    public ImageAnalyser(Config conf) {
        this.configure(conf);
    }

    public void configure(Config conf) {
        this.extractImageFeatures = conf
                .getBoolean("warc.index.extract.content.images.enabled");
        log.info("Image feature extraction = " + this.extractImageFeatures);

        this.max_size_bytes = conf.getBytes("warc.index.extract.content.images.maxSizeInBytes");
        log.info("Image - max size in bytes " + this.max_size_bytes);
        this.sampleRate = 1.0 / conf
                .getInt("warc.index.extract.content.images.analysisSamplingRate");
        log.info("Image sample rate " + this.sampleRate);
    }

    @Override
    public boolean shouldProcess(String mime) {
        if( mime.startsWith( "image" ) ) {
            if (this.extractImageFeatures) {
                return true;
            }
        }
        return false;
    }

    /* (non-Javadoc)
     * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
     */
    @Override
    public void analyse(String source, ArchiveRecordHeader header,
            InputStream tikainput,
            SolrRecord solr) {
        // Set up metadata object to pass to parsers:
        Metadata metadata = new Metadata();
        // Skip large images:
        if( header.getLength() > max_size_bytes ) {
            return;
        }
        
        
        
        // Only attempt to analyse a random sub-set of the data:
        // (prefixing with static test of a final value to allow JIT to fully
        // optimise out the "OR Math.random()" bit)
        if (sampleRate >= 1.0 || Math.random() < sampleRate) {
            // Increment number of images sampled:
            sampleCount++;
                        
            // images are enabled, we still want to extract image/height (fast)
                //This method takes 0.2ms for a large image. I can be done even faster if needed(but more complicated)).
                //see https://stackoverflow.com/questions/672916/how-to-get-image-height-and-width-using-java
              
              ImageInputStream input=null;
              ImageReader reader = null;
              try{
                input = ImageIO.createImageInputStream(tikainput);
                reader = ImageIO.getImageReaders(input).next();
                reader.setInput(input);
                 // Get dimensions of first image in the stream, without decoding pixel values
                 int width = reader.getWidth(0);
                 int height = reader.getHeight(0);
                
               // Store basic image data:
              solr.addField(SolrFields.IMAGE_HEIGHT, ""+height);
              solr.addField(SolrFields.IMAGE_WIDTH,""+width);
              solr.addField(SolrFields.IMAGE_SIZE,""+(height*width));                            
              }
              catch(Exception e){
                //it is known that (most) .ico and (all) .svg are not supported by java. Do not log, since it will spam.
               // log.warn("Unable to extract image height/width/size for url:"+header.getUrl(),e);
                
              }
              finally {
                 if (reader != null){
                   reader.dispose();
                 }
             }
              
        }
    }

    public long getSampleCount() {
        return this.sampleCount;
    }

}
