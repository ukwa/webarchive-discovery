/**
 * 
 */
package uk.bl.wap.util;

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
    private InputStream inputStream;

    public InputStreamDataSource(InputStream inputStream) {
        this.inputStream = inputStream;
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return inputStream;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getContentType() {
        return "*/*";
    }

    @Override
    public String getName() {
        return "InputStreamDataSource";
    }
}