package uk.bl.wa.hadoop.indexer;

/**
 * Purloined from the TitleLevelMetadata project.
 * @author rcoram
 */

import java.util.Collection;

import org.apache.solr.common.SolrInputDocument;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.output.Format;
import org.jdom.output.XMLOutputter;

@SuppressWarnings("unchecked")
public class MetadataBuilder {
    public static final String[] fieldNames = { "id", "collections", "url",
        "domain", "title", "crawl_date" };
    public static final String[] mandatoryFieldNames = { "id", "url", "domain",
        "crawl_date" };

    public static final Namespace oa = Namespace
        .getNamespace("http://www.openarchives.org/OAI/2.0/");
    public static final Namespace oaidc = Namespace.getNamespace("oai_dc",
        "http://www.openarchives.org/OAI/2.0/oai_dc/");
    public static final Namespace dc = Namespace.getNamespace("dc",
        "http://purl.org/dc/elements/1.1/");
    private static XMLOutputter output = new XMLOutputter(Format.getPrettyFormat());

    public static String SolrDocumentToElement(SolrInputDocument doc) {
    Collection<String> fields = doc.getFieldNames();
    for (String field : mandatoryFieldNames) {
        if (!fields.contains(field)) {
        System.err.println("Missing field '" + field + "' for "
            + doc.getFieldValues("url").toArray()[0]);
        return null;
        }
    }
    Element record = new Element("record", oa);
    Element header = new Element("header", oa);
    Element identifier = new Element("identifier", oa);
    identifier.setText((String) doc.getFieldValues("id").toArray()[0]);
    header.getChildren().add(identifier);
    record.getChildren().add(header);

    Element metdata = new Element("metadata", oa);
    Element oai_dc = new Element("dc", oaidc);

    if (doc.containsKey("collections")) {
        Element subject;
        Object[] collections = doc.getFieldValues("collections").toArray();
        for (int i = 0; i < collections.length; i++) {
        subject = new Element("subject", dc);
        subject.setText((String) collections[i]);
        oai_dc.getChildren().add(subject);
        }
    }

    Element source = new Element("source", dc);
    source.setText((String) doc.getFieldValues("url").toArray()[0]);
    oai_dc.getChildren().add(source);

    Element publisher = new Element("publisher", dc);
    publisher.setText((String) doc.getFieldValues("domain").toArray()[0]);
    oai_dc.getChildren().add(publisher);

    Element title = new Element("title", dc);
    if (doc.containsKey("title")) {
        title.setText((String) doc.getFieldValues("title").toArray()[0]);
    } else {
        title.setText((String) doc.getFieldValues("domain").toArray()[0]);
    }
    oai_dc.getChildren().add(title);

    Element date = new Element("date", dc);
    String jDate = (String) doc.getFieldValues("crawl_date").toArray()[0];
    date.setText(jDate);
    oai_dc.getChildren().add(date);

    metdata.getChildren().add(oai_dc);
    record.getChildren().add(metdata);

    return output.outputString(record);
    }
}

