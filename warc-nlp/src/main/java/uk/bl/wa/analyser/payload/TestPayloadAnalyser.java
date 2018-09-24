/**
 * 
 */
package uk.bl.wa.analyser.payload;

import java.io.InputStream;
import java.util.List;

import org.archive.io.ArchiveRecordHeader;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import uk.bl.wa.solr.SolrRecord;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class TestPayloadAnalyser extends AbstractPayloadAnalyser {

    public TestPayloadAnalyser() {
    }

    @Override
    public boolean shouldProcess(String mimeType) {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see uk.bl.wa.analyser.payload.AbstractPayloadAnalyser#analyse(org.archive.io.ArchiveRecordHeader, java.io.InputStream, uk.bl.wa.solr.SolrRecord)
     */
    @Override
    public void analyse(ArchiveRecordHeader arg0, InputStream arg1,
            SolrRecord arg2) {
        // TODO Auto-generated method stub

    }


    /**
     * Just for testing.
     * 
     * @param ignored
     */
    public static void main(String[] ignored) {

        // Get the config:
        Config conf = ConfigFactory.load();

        // create a new provider and call getMessage()
        List<AbstractPayloadAnalyser> providers = AbstractPayloadAnalyser
                .getPayloadAnalysers(conf);
        for (AbstractPayloadAnalyser provider : providers) {
            System.out.println(provider.getClass().getCanonicalName());
        }
    }

}
