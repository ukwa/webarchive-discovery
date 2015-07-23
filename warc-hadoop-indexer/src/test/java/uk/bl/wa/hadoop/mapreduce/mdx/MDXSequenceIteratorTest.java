package uk.bl.wa.hadoop.mapreduce.mdx;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

public class MDXSequenceIteratorTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testMDXOutput() throws Exception {

		// Check contents of the output:
		Iterator<MDX> i = new MDXSequenceIterator(
				WARCMDXGeneratorIntegrationTest.outputSeq);
		MDX mdx;
		while (i.hasNext()) {
			mdx = i.next();
			System.out.println("Key is: " + mdx.getHash() + " record_type: "
					+ mdx.getRecordType() + " SURT: " + mdx.getUrlAsSURT());
		}
	}

}
