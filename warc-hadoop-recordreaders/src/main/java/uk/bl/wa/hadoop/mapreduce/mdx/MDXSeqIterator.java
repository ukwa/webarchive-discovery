package uk.bl.wa.hadoop.mapreduce.mdx;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.json.JSONException;

/**
 * This helper class can iterate through a sequence file that holds MDX records.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MDXSeqIterator implements Iterator<MDX> {

	private Reader reader;
	private Text key;
	private Text value;
	private boolean hasNext;

	public MDXSeqIterator(File seq) throws IOException,
			InstantiationException, IllegalAccessException {
		Configuration config = new Configuration();
		Path path = new Path(seq.getAbsolutePath());
		reader = new SequenceFile.Reader(FileSystem.get(config), path, config);
		key = (Text) reader.getKeyClass().newInstance();
		value = (Text) reader.getValueClass().newInstance();
		// Queue up:
		hasNext = reader.next(key, value);
	}

	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public MDX next() {
        MDX mdx;
        try {
            mdx = new MDX(value.toString());
        } catch (JSONException e1) {
            mdx = null;
        }
		try {
			hasNext = reader.next(key, value);
		} catch (IOException e) {
			hasNext = false;
		}
		return mdx;
	}

	@Override
	public void remove() {
	}

}
