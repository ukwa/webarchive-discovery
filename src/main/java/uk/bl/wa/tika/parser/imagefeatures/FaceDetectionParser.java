/**
 * 
 */
package uk.bl.wa.tika.parser.imagefeatures;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.openimaj.image.DisplayUtilities;
import org.openimaj.image.FImage;
import org.openimaj.image.ImageUtilities;
import org.openimaj.image.MBFImage;
import org.openimaj.image.colour.RGBColour;
import org.openimaj.image.colour.Transforms;
import org.openimaj.image.pixel.statistics.HistogramModel;
import org.openimaj.image.processing.face.detection.DetectedFace;
import org.openimaj.image.processing.face.detection.FaceDetector;
import org.openimaj.image.processing.face.detection.HaarCascadeDetector;
import org.openimaj.image.processing.face.detection.keypoints.FKEFaceDetector;
import org.openimaj.image.processing.face.detection.keypoints.FacialKeypoint;
import org.openimaj.image.processing.face.detection.keypoints.KEDetectedFace;
import org.openimaj.math.geometry.shape.Rectangle;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class FaceDetectionParser extends AbstractParser {

	/** */
	private static final long serialVersionUID = -773080986108106790L;
	
	private static final String FACE_FRAGMENT_ID = "DETECTED_FACES";
	
	private static final Set<MediaType> SUPPORTED_TYPES =
			Collections.unmodifiableSet(new HashSet<MediaType>(Arrays.asList(
					MediaType.image("jpg"))));

	/* (non-Javadoc)
	 * @see org.apache.tika.parser.Parser#getSupportedTypes(org.apache.tika.parser.ParseContext)
	 */
	@Override
	public Set<MediaType> getSupportedTypes(ParseContext context) {
		// TODO Auto-generated method stub
		return SUPPORTED_TYPES;
	}

	/* (non-Javadoc)
	 * @see org.apache.tika.parser.Parser#parse(java.io.InputStream, org.xml.sax.ContentHandler, org.apache.tika.metadata.Metadata, org.apache.tika.parser.ParseContext)
	 */
	@Override
	public void parse(InputStream stream, ContentHandler handler,
			Metadata metadata, ParseContext context) throws IOException,
			SAXException, TikaException {
		// Parse the image:
		MBFImage image = ImageUtilities.readMBF(stream);
		// Calculate image histogram:
		//HistogramModel model = new HistogramModel(4, 4, 4);
		//model.estimateModel(image);
		//System.out.println("HIST: "+model.getFeatureVector());
		// Detect faces:
		FaceDetector<KEDetectedFace,FImage> fd = new FKEFaceDetector(20);
		//FaceDetector<DetectedFace,FImage> fd = new HaarCascadeDetector(20);
		FImage fim = Transforms.calculateIntensity( image );
		List<KEDetectedFace> faces = fd.detectFaces( fim );
		for( KEDetectedFace face : faces ) {
			for( FacialKeypoint kp : face.getKeypoints() ) {
				kp.position.translate(face.getBounds().getTopLeft());
				//image.drawPoint(kp.position, RGBColour.GRAY, 3);
			}
			Rectangle b = face.getBounds();
			image.drawShape(b, RGBColour.RED);
			// Output in standard form: http://www.w3.org/2008/WebVideo/Fragments/WD-media-fragments-spec/#naming-space
			String xywh="xywh="+(int)b.x+","+(int)b.y+","+(int)b.width+","+(int)b.height;
			metadata.add(FACE_FRAGMENT_ID, xywh);
			
		}
		DisplayUtilities.display(image);
	}

	/**
	 * @param args
	 * @throws TikaException 
	 * @throws SAXException 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException, SAXException, TikaException {
		FaceDetectionParser p = new FaceDetectionParser();
		Metadata md = new Metadata();
		//
		// http://www.flickr.com/photos/usnationalarchives/8161390041/sizes/z/in/set-72157631944278536/
		//
		p.parse(new FileInputStream("src/test/resources/faces/8161390041_1113e4e63d_z.jpg"), null, md, null);
		p.parse(new FileInputStream("src/test/resources/faces/4185781866_0e3a5f0479_o.gif"), null, md, null);
		p.parse(new FileInputStream("src/test/resources/faces/7496390584_f5b79f293a_n.jpg"), null, md, null);
		for( String face : md.getValues(FACE_FRAGMENT_ID) ) {
			System.out.println("#" + face);			
		}
	}

}
