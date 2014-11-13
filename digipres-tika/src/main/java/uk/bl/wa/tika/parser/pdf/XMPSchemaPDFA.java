package uk.bl.wa.tika.parser.pdf;

import org.apache.jempbox.xmp.XMPMetadata;
import org.apache.jempbox.xmp.XMPSchema;
import org.w3c.dom.Element;

public class  XMPSchemaPDFA extends XMPSchema {
    /**
     * The namespace for this schema.
     */
    public static final String NAMESPACE = "http://www.aiim.org/pdfa/ns/id/";
    
    /**
     * Construct a new blank PDF schema.
     *
     * @param parent The parent metadata schema that this will be part of.
     */
    public XMPSchemaPDFA( XMPMetadata parent )
    {
        super( parent, "pdfaid", NAMESPACE );
    }
    
    /**
     * Constructor from existing XML element.
     * 
     * @param element The existing element.
     * @param prefix The schema prefix.
     */
    public XMPSchemaPDFA( Element element, String prefix )
    {
        super( element, prefix );
    }
    
    /**
     * Get the PDFA Part
     *
     * @return The PDFA Part
     */
    public String getPart()
    {
        return getTextProperty( prefix + ":part" );
    }
    
    /**
     * Get the PDFA Part
     *
     * @return The PDFA Part
     */
    public String getConformance()
    {
        return getTextProperty( prefix + ":conformance" );
    }
}