package uk.bl.wa.tika.parser.imagefeatures;

/*
 * #%L
 * digipres-tika
 * %%
 * Copyright (C) 2013 - 2020 The webarchive-discovery project contributors
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
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wa.analyser.payload.AbstractPayloadAnalyser;

public class FaceDetectionParserTest {

    FaceDetectionParser p = new FaceDetectionParser(ConfigFactory.load());

    @Test
    public void checkServiceLoader() {
        // Get the config:
        Config conf = ConfigFactory.load();

        // create a new provider and call getMessage()
        List<AbstractPayloadAnalyser> providers = AbstractPayloadAnalyser
                .getPayloadAnalysers(conf);
        List<String> providerNames = new ArrayList<String>();
        for (AbstractPayloadAnalyser provider : providers) {
            // System.out.println(provider.getClass().getCanonicalName());
            providerNames.add(provider.getClass().getCanonicalName());
        }

        assertTrue("Face detection analyser not found.", providerNames
                .contains("uk.bl.wa.analyser.payload.FaceDetectionAnalyser"));
        assertTrue("Standard HTML analyser found.", providerNames
                .contains("uk.bl.wa.analyser.payload.HTMLAnalyser"));
    }

    @Test
    public void test() throws FileNotFoundException, IOException, SAXException,
            TikaException {

        // Check for faces

        // Cats:
        processImage(
                "/facedetection/cat/12553003_1844755782417745_9034032833524431014_n.jpg",
                1, 0);
        processImage(
                "/facedetection/cat/ChristmasSocks.jpg", 1,
                0);
        processImage("/facedetection/cat/Socks.jpg", 1, 0);
        processImage(
                "/facedetection/cat/intellectual_Socks.jpg",
                1, 0);
        processImage(
                "/facedetection/cat/28000980779_6289bdd986_z.jpg",
                1, 0);
        // Humans:
        processImage(
                "/facedetection/human/40699684420_1fc2338351_z.jpg",
                0, 1);
        processImage(
                "/facedetection/human/anj.jpg", 0, 1);
        processImage(
                "/facedetection/human/31090844735_a35bbb7035_z.jpg",
                0, 8);
        // Neither;
        processImage(
                "/facedetection/none/43300752325_aeaf023916_z.jpg",
                0, 0);
    }

    protected void processImage(String source, int cats, int humans)
            throws FileNotFoundException,
            IOException, SAXException, TikaException {
        System.out.println("Processing " + source);
        InputStream input = getClass().getResourceAsStream(source);
        Metadata md = new Metadata();
        p.parse(input, null, md, null);
        // Look for human faces:
        String[] faces = md.getValues(FaceDetectionParser.FACE_FRAGMENT_ID);
        List<String> human_faces = new ArrayList<String>();
        List<String> cat_faces = new ArrayList<String>();
        for (String face : faces) {
            // System.out.println("Face: " + face);
            if (face.startsWith("cat")) {
                cat_faces.add(face);
            } else {
                human_faces.add(face);
            }
        }
        assertEquals("Did not match expected number of human faces.", humans,
                human_faces.size());
        // Look for cat faces:
        assertEquals("Did not match expected number of cat faces.", cats,
                cat_faces.size());
    }

}
