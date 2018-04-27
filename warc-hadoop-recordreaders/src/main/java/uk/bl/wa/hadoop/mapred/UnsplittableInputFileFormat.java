/**
 * 
 */
package uk.bl.wa.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * 
 * Implemented using the older API in order to be compatible with Hadoop
 * Streaming.
 * 
 * Note that to use multiple records in streaming mode (as we do in the
 * uk.bl.wa.hadoop.mapreduce.lib.input.UnsplittableInputFileFormat) the streamed
 * process would have to cope with newlines coming up in the middle of the byte
 * streams. This could only work if we split on GZip record boundaries, which
 * would require a more complex implementation (like
 * https://github.com/helgeho/HadoopConcatGz but using the old API).
 * 
 * Hence, this version just tries to read the whole file into RAM.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
@SuppressWarnings("deprecation")
public class UnsplittableInputFileFormat
        extends FileInputFormat<Path, BytesWritable> {

    @Override
    public RecordReader<Path, BytesWritable> getRecordReader(InputSplit inputSplit,
            JobConf conf, Reporter reporter) throws IOException {
        return new ByteBlockRecordReader(inputSplit, conf);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.FileInputFormat#isSplitable()
     */
    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

}
