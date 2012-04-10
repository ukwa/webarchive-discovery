/**
 * 
 */
package uk.bl.wap.hadoop;

import java.io.IOException;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.RecordReader;

/**
 * Experimental code, aiming to test if a TAR file reader can be implemented directly as a RecordReader.
 * 
 * @author AnJackson
 */
public class TarFileRecordReader <Key extends WritableComparable<?>, Value extends Writable> implements RecordReader<Text, ArchiveEntry> {

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Text createKey() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ArchiveEntry createValue() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getPos() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getProgress() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean next(Text arg0, ArchiveEntry arg1) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}


}
