/**
 * 
 */
package uk.bl.wa.hadoop.mapred;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

/**
 * 
 * Streaming-API compatible version of the binary unsplittable file reader.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
@SuppressWarnings("deprecation")
public class ByteBlockRecordReader
        implements RecordReader<Path, BytesWritable> {
    private static final Log log = LogFactory
            .getLog(ByteBlockRecordReader.class);

    private FSDataInputStream fsdis;
    private Path path;
    private BytesWritable buf = new BytesWritable();
    private long bytes_read = 0;
    private long file_length = 0;
    private int buf_size = 1000 * 1000;

    /**
     * 
     * @param inputSplit
     * @param conf
     * @throws IOException
     */
    public ByteBlockRecordReader(InputSplit inputSplit, JobConf conf)
            throws IOException {
        if (inputSplit instanceof FileSplit) {
            FileSplit fs = (FileSplit) inputSplit;
            path = fs.getPath();
            FileSystem fSys = path.getFileSystem(conf);
            fsdis = fSys.open(path);
            file_length = fSys.getContentSummary(path).getLength();
        } else {
            log.error("Only FileSplit supported!");
            throw new IOException("Need FileSplit input...");
        }
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#close()
     */
    @Override
    public void close() throws IOException {
        fsdis.close();
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createKey()
     */
    @Override
    public Path createKey() {
        return path;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#createValue()
     */
    @Override
    public BytesWritable createValue() {
        return buf;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getPos()
     */
    @Override
    public long getPos() throws IOException {
        return bytes_read;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#getProgress()
     */
    @Override
    public float getProgress() throws IOException {
        return bytes_read / ((float) file_length);
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapred.RecordReader#next(java.lang.Object, java.lang.Object)
     */
    @Override
    public boolean next(Path arg0, BytesWritable arg1) throws IOException {
        byte[] bytes = new byte[buf_size];
        // Attempt to read a chunk:
        int count = fsdis.read(bytes);
        // If we're out of bytes, report that:
        if (count == -1) {
            buf = null;
            return false;
        }
        bytes_read += count;
        // Otherwise, push the new bytes into the BytesWritable:
        buf = new BytesWritable(Arrays.copyOfRange(bytes, 0, count));
        return true;
    }

}
