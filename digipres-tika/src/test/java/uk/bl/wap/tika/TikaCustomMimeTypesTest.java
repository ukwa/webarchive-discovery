package uk.bl.wap.tika;

/*
 * #%L
 * digipres-tika
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

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import org.apache.tika.Tika;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TikaCustomMimeTypesTest {
    
    private Tika tika;

    private static HashMap<String,String> tests = new HashMap<String,String>();
    
    static {
        tests.put("/spectrum/MANIC.TAP",
                "application/x-spectrum-tap; version=basic");
        tests.put("/spectrum/Manic Miner.tzx", "application/x-spectrum-tzx");
        tests.put("/wpd/TOPOPREC.WPD",
                "application/vnd.wordperfect; version=6.x");
        tests.put("/simple.pdf", "application/pdf");
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
            InputStream input = getClass().getResourceAsStream(file);
            String type = tika.detect(input);
            assertEquals(tests.get(file),type);
        }
    }

}
