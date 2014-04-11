package uk.bl.wa.apache.solr.hadoop;
///**
// * 
// */
//package org.apache.solr.hadoop;
//
//import java.io.IOException;
//
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//
//import uk.bl.wa.hadoop.WritableArchiveRecord;
//
///**
// * @author Andrew Jackson <Andrew.Jackson@bl.uk>
// *
// */
//public class WebArchiveFileInputFormat extends
//		FileInputFormat<Text, WritableArchiveRecord> {
//
//	@Override
//	public RecordReader<Text, WritableArchiveRecord> createRecordReader(
//			InputSplit split, TaskAttemptContext context) throws IOException,
//			InterruptedException {
//		WebArchiveFileRecordReader rr = new WebArchiveFileRecordReader();
//		rr.initialize(split, context);
//		return rr;
//
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see
//	 * org.apache.hadoop.mapreduce.lib.input.FileInputFormat#isSplitable(org
//	 * .apache.hadoop.mapreduce.JobContext, org.apache.hadoop.fs.Path)
//	 */
//	@Override
//	protected boolean isSplitable(JobContext context, Path filename) {
//		return false;
//	}
//
// }
