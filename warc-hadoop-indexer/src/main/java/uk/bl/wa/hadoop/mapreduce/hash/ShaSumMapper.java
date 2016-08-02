/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.hash;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ShaSumMapper extends Mapper<Path, BytesWritable, Text, Text> {

    private static final Log log = LogFactory.getLog(ShaSumMapper.class);

    private Path current = null;
    private MessageDigest md;
    private long bytes_seen = 0;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.
     * Mapper.Context)
     */
    @Override
    protected void cleanup(
            Mapper<Path, BytesWritable, Text, Text>.Context context)
                    throws IOException, InterruptedException {
        super.cleanup(context);
        log.info("Cleaning up...");
        this.emit(context);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.
     * Mapper.Context)
     */
    @Override
    protected void setup(
            Mapper<Path, BytesWritable, Text, Text>.Context context)
                    throws IOException, InterruptedException {
        super.setup(context);
        //
        try {
            md = MessageDigest.getInstance("SHA-512");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
     * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    protected void map(Path key, BytesWritable value,
            Mapper<Path, BytesWritable, Text, Text>.Context context)
                    throws IOException, InterruptedException {
        if (!key.equals(current)) {
            // Extract and emit:
            this.emit(context);
            // Set up a new one:
            current = key;
            bytes_seen = 0;
            md.reset();
            log.info("Now looking at " + key);
        }
        log.info("Got some bytes, " + Hex.encodeHexString(value.getBytes()));
        log.info("Got some bytes, size = " + value.getBytes().length);
        md.update(value.getBytes(), 0, value.getLength());
        bytes_seen += value.getLength();
        log.info("Consumed " + bytes_seen + " bytes...");
    }

    private void emit(Context context) {
        if (current == null)
            return;
        // Otherwise:
        try {
            Text name = new Text(current.getName());
            Text hex = new Text(Hex.encodeHexString(md.digest()));
            log.info("Got " + name + " " + hex + " from " + bytes_seen);
            context.write(name, hex);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
