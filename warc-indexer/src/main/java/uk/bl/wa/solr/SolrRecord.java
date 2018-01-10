/**
 * 
 */
package uk.bl.wa.solr;

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

import java.io.IOException;
import java.io.Serializable;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.util.Instrument;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrRecord implements Serializable {

	// private static Log log = LogFactory.getLog(SolrRecord.class);

	private static final long serialVersionUID = -4556484652176976470L;
	
	private SolrInputDocument doc = new SolrInputDocument();

	public String toXml() {
		return ClientUtils.toXML( doc );
	}

	/**
	 * Write the SolrDocument to the provided writer, sans XML-header.
	 * Intended for creating batches of documents.
	 */
	public void writeXml(Writer writer) throws IOException {
		ClientUtils.writeXML( doc, writer );
	}

    private static final int MAX_FIELD_LEN = 4096;
	
	public SolrRecord() {
	}

	public SolrRecord(String filename, ArchiveRecordHeader header) {
		setField(SolrFields.ID,
				"exception-at-" + filename + "@" + header.getOffset());
		setField(SolrFields.SOURCE_FILE, filename);
		setField(SolrFields.SOURCE_FILE_OFFSET, "" + header.getOffset());
		setField(SolrFields.SOLR_URL, header.getUrl());
		setField(SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_UNKNOWN);
	}

	/**
	 * Remove control characters, nulls etc,
	 * 
	 * @param value
	 * @return
	 */
	private String removeControlCharacters( String value ) {
        final long start = System.nanoTime();
		try {
            // Avoid re-compiling the regexps in each call (just a small speed-up, but a simple one)
            return CNTRL_PATTERN.matcher(
                    SPACE_PATTERN.matcher(sanitiseUTF8(value.trim())).replaceAll(" ")
            ).replaceAll("");
//            return sanitiseUTF8(value.trim().replaceAll("\\p{Space}", " ")
//         					.replaceAll("\\p{Cntrl}", ""));
		} catch (CharacterCodingException e) {
			return "";
		} finally {
            Instrument.timeRel("WARCIndexerCommand.parseWarcFiles#solrdocCreation",
							   "SolrRecord.removeControlCharacters#total", start);
        }
	}
    private static final Pattern SPACE_PATTERN = Pattern.compile("\\p{Space}");
    private static final Pattern CNTRL_PATTERN = Pattern.compile("\\p{Cntrl}");

	/**
	 * Aim to prevent "Invalid UTF-8 character 0xfffe" slipping into the text
	 * payload.
	 * 
	 * The encodes and decodes a String that may not be UTF-8 compliant as
	 * UTF-8. Any dodgy characters are replaced.
	 * 
	 * @param value
	 * @return
	 * @throws CharacterCodingException
	 */
    // It would be nice to re-use the encoder & decoder, but they are not Thread-safe
	private CharSequence sanitiseUTF8(String value) throws CharacterCodingException {
        final long start = System.nanoTime();
        try  {
            // Take a string, map it to bytes as UTF-8:
            CharsetEncoder encoder = Charset.forName("UTF-8").newEncoder();
            encoder.onMalformedInput(CodingErrorAction.REPLACE);
            encoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
            ByteBuffer bytes = encoder.encode(CharBuffer.wrap(value));
            // Now decode back again:
            CharsetDecoder decoder = Charset.forName("UTF-8").newDecoder();
            decoder.onMalformedInput(CodingErrorAction.REPLACE);
            decoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
            // No need to do toString as the CharBuffer can be used directly by the matcher in removeControlCharacters
            return decoder.decode(bytes);
        } finally {
            Instrument.timeRel("SolrRecord.removeControlCharacters#total", "SolrRecord.sanitiseUTF8", start);
        }
	}


	/**
	 * Also shorten to avoid bad data filling 'small' fields with 'big' data.
	 * 
	 * @param value
	 * @return
	 */
	private String sanitizeString( String solr_property, String value ) {
		if( ! solr_property.equals( SolrFields.SOLR_EXTRACTED_TEXT ) ) {
			if( value.length() > MAX_FIELD_LEN ) {
				value = value.substring(0, MAX_FIELD_LEN);
			}
		}
		return removeControlCharacters(value);
	}

	/**
	 * Add any non-null string properties, stripping control characters if present.
	 * 
	 * @param solr_property
	 * @param value
	 */
	public void addField( String solr_property, String value ) {
		if( value != null && !(value = sanitizeString(solr_property, value)).isEmpty())
			doc.addField( solr_property, value );
	}

	/**
	 * Set instead of adding fields.
	 * 
	 * @param solr_property
	 * @param value
	 */
	public void setField( String solr_property, String value ) {
        if( value != null && !(value = sanitizeString(solr_property, value)).isEmpty())
			doc.setField( solr_property, value );
	}
	
	/**
	 * Like add, but also allows these values to merge with those in the index already.
	 * 
	 * @param solr_property
	 * @param value
	 */
	public void mergeField( String solr_property, String value ) {
        if (value == null || value.isEmpty()) {
            return;
        }
		Map<String, String> operation = new HashMap<String, String>();
		operation.put("add", value );
		doc.addField(solr_property, operation);
	}

	/**
	 * @param fieldname
	 * @return
	 */
	public Object getFieldValue(String fieldname) {
		return doc.getFieldValue(fieldname);
	}

	/**
	 * @return
	 */
	public SolrInputDocument getSolrDocument() {
		return doc;
	}

	/**
	 * @param fieldname
	 */
	public void removeField(String fieldname) {
		doc.removeField(fieldname);
	}

	/**
	 * @param fieldname
	 * @return
	 */
	public SolrInputField getField(String fieldname) {
		return doc.getField(fieldname);
	}

	/**
	 * @param fieldname
	 * @return
	 */
	public boolean containsKey(String fieldname) {
		return doc.containsKey(fieldname);
	}

	/**
	 * @param newdoc
	 */
	public void setSolrDocument(SolrInputDocument newdoc) {
		doc = newdoc;
	}

	/**
	 * 
	 * @param e
	 */
	public void addParseException(Throwable e) {
		addField(SolrFields.PARSE_ERROR,
				e.getClass().getName() + ": " + e.getMessage());
	}

	/**
	 * 
	 * @param hint
	 * @param e
	 */
	public void addParseException(String hint, Throwable e) {
		addField(SolrFields.PARSE_ERROR, e.getClass().getName() + " " + hint
				+ ": " + e.getMessage());
	}

	/**
	 * 
	 * @return
	 */
	public String getUrl() {
		return (String) getField(SolrFields.SOLR_URL).getFirstValue();
	}

	/**
	 * 
	 * @return
	 */
	public String getWaybackDate() {
		return (String) getField(SolrFields.WAYBACK_DATE).getFirstValue();
	}

	/**
	 * 
	 * @return
	 */
	public String getHash() {
		return (String) getField(SolrFields.HASH).getFirstValue();
	}
}
