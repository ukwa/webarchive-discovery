/**
 * 
 */
package uk.bl.wa.util;

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
import java.io.InputStream;
import java.io.OutputStream;

import javax.activation.DataSource;

/**
 * 
 * An DataSource for an InputStream.
 * 
 * From http://stackoverflow.com/questions/14558440/mtom-streaming-custom-inputstreamdatasource
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class InputStreamDataSource implements DataSource {
    private boolean read = false;
    
    private InputStream inputStream;
    
    private String contentType = "*/*";

    public InputStreamDataSource(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    public InputStreamDataSource(InputStream inputStream, String contentType ) {
        this.inputStream = inputStream;
        this.contentType = contentType;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if( read == false ) {
            read = true;
            return inputStream;
        } else if( inputStream.markSupported()) {
            inputStream.reset();
            return inputStream;
        } else {
            throw new IOException("InputStreamDataSource: Cannot re-initialise this InputStream!");
        }
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getContentType() {
        return contentType;
    }

    @Override
    public String getName() {
        return "InputStreamDataSource";
    }
}
