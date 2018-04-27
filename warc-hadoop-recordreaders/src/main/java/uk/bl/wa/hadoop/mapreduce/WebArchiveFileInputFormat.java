/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import uk.bl.wa.hadoop.WritableArchiveRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WebArchiveFileInputFormat
        extends FileInputFormat<Text, WritableArchiveRecord> {

    @Override
    public RecordReader<Text, WritableArchiveRecord> createRecordReader(
            InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new WebArchiveRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

}
