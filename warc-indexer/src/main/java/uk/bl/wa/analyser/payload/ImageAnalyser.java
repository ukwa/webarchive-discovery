/**
 * 
 */
package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
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

import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.metadata.Metadata;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.tika.parser.imagefeatures.FaceDetectionParser;

import com.typesafe.config.Config;

/**
 * @author anj
 *
 */
public class ImageAnalyser extends AbstractPayloadAnalyser {
	private static Log log = LogFactory.getLog( ImageAnalyser.class );

	/** Maximum file size of images to attempt to parse */
	private final long max_size_bytes;

	/** Random sampling rate */
	private final double sampleRate;
	private long sampleCount = 0;

	/** */
	private final boolean extractFaces = true;

	/** */
	FaceDetectionParser fdp;

	public ImageAnalyser(Config conf) {
		this.max_size_bytes = conf.getBytes("warc.index.extract.content.images.maxSizeInBytes");
		this.sampleRate = 1.0 / conf
				.getInt("warc.index.extract.content.images.analysisSamplingRate");
		fdp = new FaceDetectionParser(conf);
	}

	/* (non-Javadoc)
	 * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.util.solr.SolrRecord)
	 */
	@Override
	public void analyse(ArchiveRecordHeader header, InputStream tikainput,
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

			// Attempt to extract faces:
			if (this.extractFaces) {
				ParseRunner parser = new ParseRunner(fdp, tikainput, metadata,
						solr);
				Thread thread = new Thread(parser, Long.toString(System
						.currentTimeMillis()));
				try {
					thread.start();
					thread.join(30000L);
					thread.interrupt();
				} catch (Exception e) {
					log.error("WritableSolrRecord.extract(): " + e.getMessage());
					solr.addField(
							SolrFields.PARSE_ERROR,
							e.getClass().getName()
									+ " when parsing for faces: "
									+ e.getMessage());
				}
				// Store basic image data:
				solr.addField(SolrFields.IMAGE_HEIGHT,
						metadata.get(FaceDetectionParser.IMAGE_HEIGHT));
				solr.addField(SolrFields.IMAGE_WIDTH,
						metadata.get(FaceDetectionParser.IMAGE_WIDTH));
				solr.addField(SolrFields.IMAGE_SIZE,
						metadata.get(FaceDetectionParser.IMAGE_SIZE));
				// Store faces in SOLR:
				for (String face : metadata
						.getValues(FaceDetectionParser.FACE_FRAGMENT_ID)) {
					log.debug("Found a face!");
					solr.addField(SolrFields.IMAGE_FACES, face);
				}
				int faces = metadata
						.getValues(FaceDetectionParser.FACE_FRAGMENT_ID).length;
				if (faces > 0)
					solr.setField(SolrFields.IMAGE_FACES_COUNT, "" + faces);
				// Store colour:
				solr.addField(SolrFields.IMAGE_DOMINANT_COLOUR,
						metadata.get(FaceDetectionParser.DOM_COL));
			}

		}
	}

	public long getSampleCount() {
		return this.sampleCount;
	}

}
