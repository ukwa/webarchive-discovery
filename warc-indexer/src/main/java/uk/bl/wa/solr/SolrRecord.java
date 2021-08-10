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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.util.Instrument;
import uk.bl.wa.util.Normalisation;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrRecord implements Serializable {

    // private static Logger log = LoggerFactory.getLogger(SolrRecord.class);

    private static final long serialVersionUID = -4556484652176976472L;
    
    private SolrInputDocument doc = new SolrInputDocument();

    private final int defaultMax;
    private final HashMap<String, Integer> maxLengths; // Explicit HashMap as it is Serializable

    public SolrRecord(int defaultMaxFieldLength, HashMap<String, Integer> maxFieldLengths) {
        this.defaultMax = defaultMaxFieldLength;
        this.maxLengths = maxFieldLengths;
    }

    /**
     * @deprecated use {@link SolrRecordFactory#createRecord()} instead.
     */
    @Deprecated
    public SolrRecord() {
        this(SolrRecordFactory.DEFAULT_MAX_LENGTH, new HashMap<String, Integer>());
    }

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

    public SolrRecord(int defaultMaxFieldLength, HashMap<String, Integer> maxFieldLengths,
                      String filename, ArchiveRecordHeader header) {
        defaultMax = defaultMaxFieldLength;
        maxLengths = maxFieldLengths;
        setField(SolrFields.ID,
                "exception-at-" + filename + "@" + header.getOffset());
        setField(SolrFields.SOURCE_FILE, filename);
        setField(SolrFields.SOURCE_FILE_OFFSET, "" + header.getOffset());
        setField(SolrFields.SOLR_URL, Normalisation.sanitiseWARCHeaderValue(header.getUrl()));
        setField(SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_UNKNOWN);
    }

    /**
     * @deprecated use {@link SolrRecordFactory#createRecord(String, ArchiveRecordHeader)} instead.
     */
    public SolrRecord(String filename, ArchiveRecordHeader header) {
        this(SolrRecordFactory.DEFAULT_MAX_LENGTH, new HashMap<String, Integer>(), filename, header);
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
//                             .replaceAll("\\p{Cntrl}", ""));
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
     * @param truncateToLength to truncate to (-1 means don't truncate)
     * @return
     */
    private String sanitizeString(String value, int truncateToLength) {
        if (value == null) {
            return null;
        }
        if (truncateToLength > 0) {
            if (value.length() > truncateToLength) {
                value = value.substring(0, truncateToLength);
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
    public void addField(String solr_property, String value) {
        addFieldTruncated(solr_property, value, getMaxLength(solr_property));
    }

    private int getMaxLength(String solrField) {
        Integer max = maxLengths.get(solrField);
        return max == null ? defaultMax : max;
    }

    /**
     * Add the field, truncating the value if it's larger than the given limit.
     * 
     * @param solr_property
     * @param value
     * @param truncateTo
     */
    public void addFieldTruncated(String solr_property, String value, int truncateTo) {
        value = sanitizeString(value, truncateTo);
        // If the value is not empty:
        if (value != null && !value.isEmpty()) {
            // Check if the value is already set:
            Collection<Object> values = doc.getFieldValues(solr_property);
            if (values == null || !values.contains(value)) {
                doc.addField(solr_property, value);
            }
        }
    }

    /**
     * Set instead of adding fields.
     * 
     * @param solr_property
     * @param value
     */
    public void setField(String solr_property, String value) {
        setFieldTruncated(solr_property, value, getMaxLength(solr_property));
    }

    /**
     * Set the field, truncating the value if it's larger than the given limit.
     * 
     * @param solr_property
     * @param value
     * @param truncateTo
     */
    public void setFieldTruncated(String solr_property, String value, int truncateTo) {
        value = sanitizeString(value, truncateTo);
        if (value != null && !value.isEmpty())
            doc.setField(solr_property, value);
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

    /*
     * ----------------------------------------
     * 
     * Helpers for getting data back out.
     * 
     * ----------------------------------------
     */

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

    /**
     * 
     * @return
     */
    public String getHost() {
        return (String) getField(SolrFields.SOLR_HOST).getFirstValue();
    }

    /**
     * Get a string containing the format as determined by three different
     * techniques:
     * 
     * @return
     */
    public String getFormatResults() {
        StringBuilder sb = new StringBuilder();
        // As Served:
        SolrInputField served = getField(SolrFields.CONTENT_TYPE_SERVED);
        if (served != null) {
            sb.append((String) served.getFirstValue());
        }
        // Tika:
        sb.append("\t");
        SolrInputField tika = getField(SolrFields.CONTENT_TYPE_TIKA);
        if (tika != null) {
            sb.append((String) tika.getFirstValue());
        }
        // DROID:
        sb.append("\t");
        SolrInputField droid = getField(SolrFields.CONTENT_TYPE_DROID);
        if (droid != null) {
            sb.append((String) droid.getFirstValue());
        }

        return sb.toString();
    }

    /**
     * Get the list of faces and the item identifier:
     */
    public List<String> getFaces() {
        SolrInputField faces = getField(SolrFields.IMAGE_FACES);
        if (faces == null || faces.getValueCount() == 0)
            return null;
        // Otherwise, list 'em:
        List<String> hl = new ArrayList<String>();
        this.gatherMatches(faces.getValues(), "cat", hl);
        this.gatherMatches(faces.getValues(), "human", hl);
        return hl;
    }

    private void gatherMatches(Collection<Object> strings, String prefix,
            List<String> hl) {
        StringBuilder sb = new StringBuilder();
        sb.append(getUrl());
        sb.append("\t");
        sb.append(getWaybackDate());
        sb.append("\t");
        // Order:
        List<String> list = new ArrayList<String>();
        for (Object v : strings) {
            String vs = (String) v;
            list.add(vs);
        }
        Collections.sort(list);
        // Go through:
        int i = 0;
        for (String vs : list) {
            if (i > 0)
                sb.append(" ");
            if (vs.startsWith(prefix)) {
                sb.append(vs);
                i++;
            }
        }
        if (i > 0) {
            hl.add(sb.toString());
        }
    }

    /**
     * Get the host->host links:
     */
    public List<String> getHostLinks() {
        SolrInputField links = getField(SolrFields.SOLR_LINKS_HOSTS);
        if (links == null || links.getValueCount() == 0)
            return null;

        // Otherwise, build a list:
        List<String> hl = new ArrayList<String>();
        for (Object v : links.getValues()) {
            hl.add(getHost() + "\t" + (String) v);
        }
        return hl;
    }

    /**
     * Iterates the fields of the contained {@link org.apache.solr.common.SolrDocument} and calculates the approximate
     * amount of bytes of heaps used to hold it. This is not an exact measure!
     * @return the approximate amount of heap bytes for this SolrRecord.
     */
    public long getApproximateSize() {
        long total = 200 + maxLengths.size()*100L; // The SolrRecord itself
        total += getApproximateSize(doc);
        return total;
    }
    // This is really quick & dirty work here, sorry. If an exact measure is needed, it should be re-implemented
    private long getApproximateSize(SolrInputDocument doc) {
        long total = 100L; // the doc itself
        for (SolrInputField field: doc) {
            total += 32 + getApproximateObjectSize(field.getName());
             for (Object o: field) {
                 total += getApproximateObjectSize(o);
             }
        }
        if (doc.hasChildDocuments()) {
            for (SolrInputDocument child: doc.getChildDocuments()) {
                total += getApproximateSize(child);
            }
        }
        return total;
    }
    private long getApproximateObjectSize(Object o) {
        if (o instanceof String) {
            return 48 + ((String)o).length()*2L;
        }
        if (o instanceof Long) {
            return 128;
        }
        return 64;
    }
}
