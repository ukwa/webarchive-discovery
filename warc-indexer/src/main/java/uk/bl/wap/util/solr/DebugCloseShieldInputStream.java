/**
 * 
 */
package uk.bl.wap.util.solr;

import java.io.InputStream;

import org.apache.tika.io.CloseShieldInputStream;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 * 
 * 

INFO  TikaExtractor - Processing... http://www.opf-labs.org/format-corpus/variations/variations/application/x-iwork-pages-sffpages/09-4.1-923/lorem-ipsum.pages
java.lang.Exception
	at uk.bl.wap.util.solr.DebugCloseShieldInputStream.close(DebugCloseShieldInputStream.java:26)
	at java.io.BufferedInputStream.close(BufferedInputStream.java:451)
	at org.apache.tika.io.TikaInputStream$1.close(TikaInputStream.java:553)
	at org.apache.tika.io.TemporaryResources.close(TemporaryResources.java:121)
	at org.apache.tika.io.TikaInputStream.close(TikaInputStream.java:637)
	at java.io.PushbackInputStream.close(PushbackInputStream.java:358)
	at org.apache.commons.compress.archivers.zip.ZipArchiveInputStream.close(ZipArchiveInputStream.java:401)
	at org.apache.tika.parser.iwork.IWorkPackageParser.parse(IWorkPackageParser.java:219)
	at org.apache.tika.parser.CompositeParser.parse(CompositeParser.java:242)
	at org.apache.tika.parser.CompositeParser.parse(CompositeParser.java:242)
	at org.apache.tika.parser.AutoDetectParser.parse(AutoDetectParser.java:120)
	at uk.bl.wap.util.solr.TikaExtractor$ParseRunner.run(TikaExtractor.java:301)
	at java.lang.Thread.run(Thread.java:680)
	
 *
 */
public class DebugCloseShieldInputStream extends CloseShieldInputStream {

	public DebugCloseShieldInputStream(InputStream in) {
		super(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.tika.io.CloseShieldInputStream#close()
	 */
	@Override
	public void close() {
		super.close();
		new Exception().printStackTrace();
	}

}
