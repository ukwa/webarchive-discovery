package uk.bl.wa.tika.parser.imagefeatures;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.typesafe.config.ConfigFactory;

public class FaceDetectionParserTest {

    FaceDetectionParser p = new FaceDetectionParser(ConfigFactory.load());

    @Test
    public void test() throws FileNotFoundException, IOException, SAXException,
            TikaException {

        // Check for faces

        // Cats:
        processImage(
                "src/test/resources/facedetection/cat/12553003_1844755782417745_9034032833524431014_n.jpg",
                1, 0);
        processImage(
                "src/test/resources/facedetection/cat/ChristmasSocks.jpg", 1,
                0);
        processImage("src/test/resources/facedetection/cat/Socks.jpg", 1, 0);
        processImage(
                "src/test/resources/facedetection/cat/intellectual_Socks.jpg",
                1, 0);
        processImage(
                "src/test/resources/facedetection/cat/28000980779_6289bdd986_z.jpg",
                1, 0);
        // Humans:
        processImage(
                "src/test/resources/facedetection/human/40699684420_1fc2338351_z.jpg",
                0, 1);
        processImage(
                "src/test/resources/facedetection/human/anj.jpg", 0, 1);
        processImage(
                "src/test/resources/facedetection/human/31090844735_a35bbb7035_z.jpg",
                0, 8);
        // Neither;
        processImage(
                "src/test/resources/facedetection/none/43300752325_aeaf023916_z.jpg",
                0, 0);
    }

    protected void processImage(String source, int cats, int humans)
            throws FileNotFoundException,
            IOException, SAXException, TikaException {
        System.out.println("Processing " + source);
        Metadata md = new Metadata();
        p.parse(new FileInputStream(source), null, md, null);
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
