package uk.bl.wa.hadoop.indexer;

import static org.junit.Assert.assertFalse;

/*-
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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wa.analyser.payload.AbstractPayloadAnalyser;

public class ServiceLoaderTest {

    @Test
    public void testServiceLoader() {
        // Get the config:
        Config conf = ConfigFactory.load();

        // create a new provider and call getMessage()
        List<AbstractPayloadAnalyser> providers = AbstractPayloadAnalyser
                .getPayloadAnalysers(conf);
        List<String> providerNames = new ArrayList<String>();
        for (AbstractPayloadAnalyser provider : providers) {
            System.out.println(provider.getClass().getCanonicalName());
            providerNames.add(provider.getClass().getCanonicalName());
        }

        assertFalse(
                "Face detection analyser found - should not be in base build.",
                providerNames
                .contains("uk.bl.wa.analyser.payload.FaceDetectionAnalyser"));
        assertTrue("Standard HTML analyser found.", providerNames
                .contains("uk.bl.wa.analyser.payload.HTMLAnalyser"));
    }


}
