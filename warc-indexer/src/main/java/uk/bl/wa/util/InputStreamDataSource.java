/**
 * 
 */
package uk.bl.wa.util;

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
    	} else {
    		throw new IOException("Cannot re-initialise this InputStream");
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