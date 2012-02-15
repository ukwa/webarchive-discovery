package uk.bl.wap.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

@SuppressWarnings( { "deprecation", "unchecked", "rawtypes" } )
public class ArchiveFileInputFormat extends FileInputFormat< LongWritable, WritableArchiveRecord > {

	@Override
	public RecordReader<LongWritable, WritableArchiveRecord> getRecordReader( InputSplit split, JobConf conf, Reporter reporter ) throws IOException {
		return new ArchiveFileRecordReader( conf, split );
	}

	@Override
	protected boolean isSplitable( FileSystem fs, Path filename ) {
		return false;
	}
}
