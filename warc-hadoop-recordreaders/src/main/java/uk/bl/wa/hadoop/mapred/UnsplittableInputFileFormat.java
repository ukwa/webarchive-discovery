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
