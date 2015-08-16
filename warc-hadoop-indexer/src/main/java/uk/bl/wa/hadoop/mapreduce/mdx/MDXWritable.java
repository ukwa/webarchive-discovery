/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.mdx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * 
 * Use a writable class wrapper rather than holding the whole MDX as text to
 * avoid lots of unnecessary deserialisation when passing records through.
 * 
 * i.e. only records being modified (like reduplication of revists) would need
 * full deserialisation.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MDXWritable implements Writable {

	private Text hash;

	private Text url;

	private Text ts;

	private Text recordType;

	private Text mdx;

	/* For Hadoop serialisation */
	private MDXWritable() {
		this.hash = new Text();
		this.url = new Text();
		this.ts = new Text();
		this.recordType = new Text();
		this.mdx = new Text();
	}

	/* For use */
	public MDXWritable(MDX mdx) {
		this.hash = new Text(mdx.getHash());
		this.url = new Text(mdx.getUrl());
		this.ts = new Text(mdx.getTs());
		this.recordType = new Text(mdx.getRecordType());
		this.mdx = new Text(mdx.toJSON());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		hash.readFields(in);
		url.readFields(in);
		ts.readFields(in);
		recordType.readFields(in);
		mdx.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		hash.write(out);
		url.write(out);
		ts.write(out);
		recordType.write(out);
		mdx.write(out);
	}

	/**
	 * @return the hash
	 */
	public Text getHash() {
		return hash;
	}

	/**
	 * @return the url
	 */
	public Text getUrl() {
		return url;
	}

	/**
	 * @return the ts
	 */
	public Text getTs() {
		return ts;
	}

	/**
	 * @return the recordType
	 */
	public Text getRecordType() {
		return recordType;
	}

	/**
	 * @return the mdx
	 */
	public MDX getMDX() {
		return MDX.fromJSONString(mdx.toString());
	}

	/**
	 * 
	 * @return the mdx as Text
	 */
	public Text getMDXAsText() {
		return mdx;
	}
}
