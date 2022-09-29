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
 * Copyright (C) 2013 - 2022 The webarchive-discovery project contributors
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
import java.util.*;
import java.util.function.UnaryOperator;

import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.archive.format.warc.WARCConstants;
import org.archive.io.ArchiveRecordHeader;

import uk.bl.wa.Memento;
import uk.bl.wa.MementoRecord;
import uk.bl.wa.util.Normalisation;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SolrRecord implements Serializable {

    // private static Logger log = LoggerFactory.getLogger(SolrRecord.class);

    private static final long serialVersionUID = -4556484652176976473L;
    
    private SolrInputDocument doc = new SolrInputDocument();

    /**
     * field name -> function map for adjusting content length, correct UTF-8 problems etc.
     */
    // Marked as transient: SolrRecord content should not be changed after serialization
    private transient Map<String, FieldAdjuster> contentAdjusters;
    /**
     * If there is no contentAdjuster available for a given field from {@link #contentAdjusters},
     * the defaultContentAdjuster is used.
     */
    // Marked as transient: SolrRecord content should not be changed after serialization
    private transient FieldAdjuster defaultContentAdjuster;

    public SolrRecord(Map<String, FieldAdjuster> contentAdjusters, FieldAdjuster defaultcontentAdjuster) {
        this.contentAdjusters = contentAdjusters;
        this.defaultContentAdjuster = defaultcontentAdjuster == null ? FieldAdjuster.PASSTHROUGH : defaultcontentAdjuster;
    }

    /**
     * @deprecated use {@link SolrRecordFactory#createRecord()} instead.
     */
    @Deprecated
    public SolrRecord() {
        this(Collections.emptyMap(), getDefaultMaxLengthAdjuster());
    }

    public SolrRecord(Map<String, FieldAdjuster> contentAdjusters, FieldAdjuster defaultContentAdjuster,
                      String filename, ArchiveRecordHeader header) {
        this(contentAdjusters, defaultContentAdjuster);
        setField(SolrFields.ID, "exception-at-" + filename + "@" + header.getOffset());
        setField(SolrFields.SOURCE_FILE, filename);
        setField(SolrFields.SOURCE_FILE_OFFSET, "" + header.getOffset());
        setField(SolrFields.SOLR_URL, Normalisation.sanitiseWARCHeaderValue(header.getUrl()));
        setField(SolrFields.SOLR_URL_TYPE, SolrFields.SOLR_URL_TYPE_UNKNOWN);
        if (!header.getHeaderFields().isEmpty()) {
            if( header.getHeaderFieldKeys().contains( WARCConstants.HEADER_KEY_TYPE ) ) {
                setField(SolrFields.SOLR_RECORD_TYPE, (String)header.getHeaderFields().get(WARCConstants.HEADER_KEY_TYPE));
            }
        }
    }

    /**
     * @deprecated use {@link SolrRecordFactory#createRecord(String, ArchiveRecordHeader)} instead.
     */
    public SolrRecord(String filename, ArchiveRecordHeader header) {
        this(Collections.emptyMap(), getDefaultMaxLengthAdjuster(), filename, header);
    }

    @SuppressWarnings("ConstantConditions")
    private static FieldAdjuster getDefaultMaxLengthAdjuster() {
        return new FieldAdjuster(
                -1,
                s -> SolrRecordFactory.DEFAULT_MAX_LENGTH < 0 || s == null ||
                     s.length() < SolrRecordFactory.DEFAULT_MAX_LENGTH ?
                        s :
                        s.substring(0, SolrRecordFactory.DEFAULT_MAX_LENGTH),
                "maxLength=" + SolrRecordFactory.DEFAULT_MAX_LENGTH
        );
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

    /**
     * Add any non-null string properties, stripping control characters if present.
     * 
     * @param solr_property
     * @param value
     */
    public void addField(String solr_property, String value) {
        String adjusted = adjust(solr_property, value);
        if (adjusted != null && isAllowedtoAdd(solr_property, adjusted)) {
            doc.addField(solr_property, adjusted);
        }
    }

    /**
     * Add the field, truncating the value if it's larger than the given limit.
     * 
     * @param solr_property
     * @param value
     * @param truncateTo ignored after deprecation.
     * @deprecated superceeded by the {@link #contentAdjusters} mechanism. Use {@link #addField(String, String)} instead.
     */
    public void addFieldTruncated(String solr_property, String value, int truncateTo) {
        addField(solr_property, value);
    }

    /**
     * Set instead of adding fields.
     * 
     * @param solr_property
     * @param value
     */
    public void setField(String solr_property, String value) {
        String adjusted = adjust(solr_property, value);
        if (adjusted != null && isAllowedtoAdd(solr_property, adjusted)) {
            doc.setField(solr_property, adjusted);
        }
    }

    /**
     * Set the field, truncating the value if it's larger than the given limit.
     * 
     * @param solr_property
     * @param value
     * @param truncateTo ignored after deprecation.
     * @deprecated superceeded by the {@link #contentAdjusters} mechanism. Use {@link #setField(String, String)} instead.
     */
    public void setFieldTruncated(String solr_property, String value, int truncateTo) {
        setField(solr_property, value);
    }
    
    /**
     * Adjusts the value using {@link #contentAdjusters} or {@link #defaultContentAdjuster}.
     * If the value is to be ignored, null is returned.
     * @param solrField the destination field name in the SolrDocument.
     * @param value the field value.
     * @return the value adjusted according to setup or null if the value should be ignored.
     */
    String adjust(String solrField, String value) {
        return contentAdjusters.getOrDefault(solrField, defaultContentAdjuster).apply(value);
    }

    /**
     * @param solrField the destination field name in the SolrDocument.
     * @param value used to check for duplicates.
     * @return true if it is acceptable to add a(nother) value to the given field.
     */
    boolean isAllowedtoAdd(String solrField, String value) {
        int maxValues = contentAdjusters.getOrDefault(solrField, defaultContentAdjuster).getMaxValues();
        if (maxValues == -1) {
            return true;
        }
        if (maxValues == 0) {
            return false;
        }
        Collection<Object> values = doc.getFieldValues(solrField);
        return values == null || (values.size() < maxValues && !values.contains(value));
    }

    /**
     * Like add, but also allows these values to merge with those in the index already.
     * 
     * @param solr_property
     * @param value
     */
    public void mergeField( String solr_property, String value ) {
        String adjusted = adjust(solr_property, value);
        if (adjusted == null) {
            return;
        }
        // Noto: No check for maxValues!
        Map<String, String> operation = new HashMap<String, String>();
        operation.put("add", adjusted );
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
        long total = 200; // + maxLengths.size()*100L; // The SolrRecord itself
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

    // Convertor class to convert to plain bean:
    public Memento toMemento() {
        Memento m = new Memento();

        m.setId((String)this.getFieldValue(SolrFields.ID));
        m.setUrl((String)this.getFieldValue(SolrFields.SOLR_URL));
        m.setRecordType((String)this.getFieldValue(SolrFields.SOLR_RECORD_TYPE));

        String statusCode = (String)this.getFieldValue(SolrFields.SOLR_STATUS_CODE);
        if( statusCode != null ) m.setStatusCode(Integer.parseInt(statusCode));

        String contentLength = (String)this.getFieldValue(SolrFields.CONTENT_LENGTH);
        if( contentLength != null ) m.setContentLength(Long.parseLong(contentLength));
        String textLength = (String)this.getFieldValue(SolrFields.SOLR_EXTRACTED_TEXT_LENGTH);
        if( textLength != null ) m.setContentTextLength(Long.parseLong(textLength));
        m.setContentLanguage((String)this.getFieldValue(SolrFields.CONTENT_LANGUAGE));

        m.setContentType((String)this.getFieldValue(SolrFields.SOLR_CONTENT_TYPE));
        m.setContentTypeDroid((String)this.getFieldValue(SolrFields.CONTENT_TYPE_DROID));
        m.setContentTypeExt((String)this.getFieldValue(SolrFields.CONTENT_TYPE_EXT));
        m.setContentTypeFull((String)this.getFieldValue(SolrFields.FULL_CONTENT_TYPE));
        m.setContentTypeNorm((String)this.getFieldValue(SolrFields.SOLR_NORMALISED_CONTENT_TYPE));
        m.setContentTypeServed((String)this.getFieldValue(SolrFields.CONTENT_TYPE_SERVED));
        m.setContentTypeTika((String)this.getFieldValue(SolrFields.CONTENT_TYPE_TIKA));
        m.setContentTypeVersion((String)this.getFieldValue(SolrFields.CONTENT_VERSION));

        return m;
    }

    public MementoRecord toMementoRecord() {
        MementoRecord m = new MementoRecord();

        m.setSourceFilePath((String)this.getFieldValue(SolrFields.SOURCE_FILE_PATH));
        m.setSourceFileOffset(Long.parseLong((String)this.getFieldValue(SolrFields.SOURCE_FILE_OFFSET)));        
        m.addMetadata(SolrFields.SOURCE_FILE, (String)this.getFieldValue(SolrFields.SOURCE_FILE));

        m.addMetadata(SolrFields.ID, (String)this.getFieldValue(SolrFields.ID));
        m.addMetadata(SolrFields.SOLR_URL, (String)this.getFieldValue(SolrFields.SOLR_URL));
        m.addMetadata(SolrFields.SOLR_RECORD_TYPE, (String)this.getFieldValue(SolrFields.SOLR_RECORD_TYPE));

        //String contentLength = (String)this.getFieldValue(SolrFields.CONTENT_LENGTH);
        //if( contentLength != null ) m.setContentLength(Long.parseLong(contentLength));
        //String textLength = (String)this.getFieldValue(SolrFields.SOLR_EXTRACTED_TEXT_LENGTH);
        //if( textLength != null ) m.setContentTextLength(Long.parseLong(textLength));
        m.addMetadata(SolrFields.CONTENT_LANGUAGE, (String)this.getFieldValue(SolrFields.CONTENT_LANGUAGE));

        m.addMetadata(SolrFields.SOLR_CONTENT_TYPE, (String)this.getFieldValue(SolrFields.SOLR_CONTENT_TYPE));
        m.addMetadata(SolrFields.CONTENT_TYPE_DROID, (String)this.getFieldValue(SolrFields.CONTENT_TYPE_DROID));
        m.addMetadata(SolrFields.CONTENT_TYPE_EXT, (String)this.getFieldValue(SolrFields.CONTENT_TYPE_EXT));
        m.addMetadata(SolrFields.FULL_CONTENT_TYPE, (String)this.getFieldValue(SolrFields.FULL_CONTENT_TYPE));
        m.addMetadata(SolrFields.SOLR_NORMALISED_CONTENT_TYPE, (String)this.getFieldValue(SolrFields.SOLR_NORMALISED_CONTENT_TYPE));
        m.addMetadata(SolrFields.CONTENT_TYPE_SERVED, (String)this.getFieldValue(SolrFields.CONTENT_TYPE_SERVED));
        m.addMetadata(SolrFields.CONTENT_TYPE_TIKA, (String)this.getFieldValue(SolrFields.CONTENT_TYPE_TIKA));
        m.addMetadata(SolrFields.CONTENT_VERSION, (String)this.getFieldValue(SolrFields.CONTENT_VERSION));

        return m;
    }
}
