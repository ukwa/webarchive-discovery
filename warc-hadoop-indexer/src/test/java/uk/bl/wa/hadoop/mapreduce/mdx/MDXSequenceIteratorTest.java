package uk.bl.wa.hadoop.mapreduce.mdx;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

public class MDXSequenceIteratorTest {

	private File mdxWarcBothSeq = new File(
			"src/test/resources/mdx-seq/mdx-warc-both.seq");

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testMDXOutput() throws Exception {

		// Check contents of the output:
		Iterator<MDX> i = new MDXSequenceIterator(mdxWarcBothSeq);
		MDX mdx;
		int counter = 0;
		while (i.hasNext()) {
			mdx = i.next();
			System.out.println("Key is: " + mdx.getHash() + " record_type: "
					+ mdx.getRecordType() + " SURT: " + mdx.getUrlAsSURT());
			counter++;
		}
		System.out.println("Total records: " + counter);
		assertEquals(114, counter);
	}

}
