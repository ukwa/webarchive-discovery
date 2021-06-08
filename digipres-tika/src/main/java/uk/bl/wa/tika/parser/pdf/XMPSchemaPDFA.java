package uk.bl.wa.tika.parser.pdf;

/*
 * #%L
 * digipres-tika
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
