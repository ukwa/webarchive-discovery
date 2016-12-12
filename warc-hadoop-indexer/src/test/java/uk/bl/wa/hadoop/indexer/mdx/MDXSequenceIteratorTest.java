package uk.bl.wa.hadoop.indexer.mdx;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import uk.bl.wa.hadoop.mapreduce.mdx.MDX;
import uk.bl.wa.hadoop.mapreduce.mdx.MDXSeqIterator;

public class MDXSequenceIteratorTest {

	private File mdxWarcBothSeq = new File(
			"src/test/resources/mdx-seq/mdx-warc-both.seq");

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testMDXOutput() throws Exception {

		// Check contents of the output:
		Iterator<MDX> i = new MDXSeqIterator(mdxWarcBothSeq);
		MDX mdx;
		int counter = 0;
		while (i.hasNext()) {
			mdx = i.next();
            // System.err.println("mdx: " + mdx);
            System.out.println("Hash is: " + mdx.getString("hash")
                    + " record_type: "
                    + mdx.getString("recordType") + " SURT: "
                    + mdx.getUrlAsSURT());
			counter++;
		}
		System.out.println("Total records: " + counter);
		assertEquals(114, counter);
	}

}
