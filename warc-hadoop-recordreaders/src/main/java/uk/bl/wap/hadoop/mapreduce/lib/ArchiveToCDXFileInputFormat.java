package uk.bl.wap.hadoop.mapreduce.lib;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ArchiveToCDXFileInputFormat extends FileInputFormat<Text, Text> {
	TextInputFormat input = null;

	@Override
	public List<InputSplit> getSplits( JobContext context ) throws IOException {
		if( input == null ) {
			input = new TextInputFormat();
		}
		return input.getSplits( context );
	}

	@Override
	public RecordReader<Text, Text> createRecordReader( InputSplit split, TaskAttemptContext context ) throws IOException, InterruptedException {
		return new DereferencingArchiveToCDXRecordReader<Text, Text>();
	}
}
