/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class UnsplittableInputFileFormat
        extends FileInputFormat<Path, BytesWritable> {

    @Override
    public RecordReader<Path, BytesWritable> createRecordReader(InputSplit arg0,
            TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new ByteBlockRecordReader();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable(org.
     * apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

}
