package uk.bl.wap.tika;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.tika.Tika;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TikaCustomMimeTypesTest {
	
	private Tika tika;

	private static HashMap<String,String> tests = new HashMap<String,String>();
	
	static {
		tests.put("src/test/resources/spectrum/MANIC.TAP", "application/x-spectrum-tap; version=basic");
		tests.put("src/test/resources/spectrum/Manic Miner.tzx", "application/x-spectrum-tzx");
        tests.put("src/test/resources/wpd/TOPOPREC.WPD",
                "application/vnd.wordperfect; version=6.x");
		tests.put("src/test/resources/simple.pdf", "application/pdf");
	}

	@Before
	public void setUp() throws Exception {
		tika = new Tika();
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws IOException {
		for( String file : tests.keySet() ) {
			File f = new File(file);
			String type = tika.detect(f);
			assertEquals(tests.get(file),type);
		}
	}

}
