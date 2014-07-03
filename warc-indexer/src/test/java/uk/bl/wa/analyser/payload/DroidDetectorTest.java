/**
 * 
 */
package uk.bl.wa.analyser.payload;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

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
	 */
	@Test
	public void testBasicDetection() throws IOException,
			CommandExecutionException {
		this.runDroids("src/test/resources/cc.png", "image/png");
		this.runDroids("src/test/resources/cc0.mp3", "audio/mpeg");
	}

	private void runDroids(String filename, String expected) throws IOException,
			CommandExecutionException {

		// Set up File and Metadata:
		File file = new File(filename);
		Metadata metadata = new Metadata();
		metadata.set(Metadata.RESOURCE_NAME_KEY, file.getName());

		// Test identification two ways:
		assertEquals("ID of " + filename + " as InputStream, failed.",
				expected, dd.detect(new FileInputStream(file), metadata)
						.getBaseType().toString());

		assertEquals("ID of " + filename + " as File, failed.", expected, dd
				.detect(file).getBaseType().toString());
	}
}
