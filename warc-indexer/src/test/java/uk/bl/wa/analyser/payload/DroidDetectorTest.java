/**
 * 
 */
package uk.bl.wa.analyser.payload;

/*
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2014 The UK Web Archive
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
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.tika.metadata.Metadata;
import org.junit.Before;
import org.junit.Test;

import uk.bl.wa.nanite.droid.DroidDetector;
import uk.gov.nationalarchives.droid.command.action.CommandExecutionException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class DroidDetectorTest {

	private DroidDetector dd;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		dd = new DroidDetector();

	}

	/**
     * 
     * @throws IOException
     * @throws CommandExecutionException
     * @throws URISyntaxException
     */
	@Test
	public void testBasicDetection() throws IOException,
            CommandExecutionException, URISyntaxException {
        String s = this.getClass().getClassLoader().getResource("").getPath();
        System.out.println("Current relative path is: " + s);
		this.runDroids("src/test/resources/cc.png", "image/png");
		this.runDroids("src/test/resources/cc0.mp3", "audio/mpeg");
	}

	private void runDroids(String filename, String expected) throws IOException,
            CommandExecutionException, URISyntaxException {

		// Set up File and Metadata:
        File file = new File(filename);
		Metadata metadata = new Metadata();
		metadata.set(Metadata.RESOURCE_NAME_KEY, file.getName());

		// Test identification two ways:
		assertEquals("ID of " + filename + " as File, failed.", expected, dd
				.detect(file).getBaseType().toString());

		assertEquals("ID of " + filename + " as InputStream, failed.",
				expected, dd.detect(new FileInputStream(file), metadata)
						.getBaseType().toString());

	}
}
