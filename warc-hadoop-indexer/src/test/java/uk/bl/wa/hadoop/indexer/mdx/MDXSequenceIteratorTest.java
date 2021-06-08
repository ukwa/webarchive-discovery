package uk.bl.wa.hadoop.indexer.mdx;

/*
 * #%L
 * warc-hadoop-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 2 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-2.0.html>.
 * #L%
 */

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
