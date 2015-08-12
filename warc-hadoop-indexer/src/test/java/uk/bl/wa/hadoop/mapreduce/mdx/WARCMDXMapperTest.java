package uk.bl.wa.hadoop.mapreduce.mdx;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.junit.Before;
import org.junit.Test;

import uk.bl.wa.hadoop.WritableArchiveRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

public class WARCMDXMapperTest {

	private static final Log LOG = LogFactory.getLog(WARCMDXMapperTest.class);

	MapDriver<Text, WritableArchiveRecord, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, Text> reduceDriver;
	MapReduceDriver<Text, WritableArchiveRecord, Text, MapWritable, Text, MapWritable> mapReduceDriver;


	@Before
	public void setUp() {
		// Overload the config:
		JobConf conf = new JobConf();
		Config c = ConfigFactory.parseResources("mdx-test.conf");
		conf.set(WARCMDXGenerator.CONFIG_PROPERTIES, c.withOnlyPath("warc")
				.root().render(ConfigRenderOptions.concise()));
		// Set up the mapper etc.:
		WARCMDXMapper mapper = new WARCMDXMapper();
		MDXSeqReduplicatingReducer reducer = new MDXSeqReduplicatingReducer();
		mapDriver = MapDriver.newMapDriver(mapper).withConfiguration(conf);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
		mapReduceDriver = MapReduceDriver.newMapReduceDriver();
	}

	@Test
	public void testMapper() throws IOException {
		
		Set<String> skippableRecords = new HashSet<String>();
		skippableRecords.add("application/warc-fields");
		skippableRecords.add("text/dns");

		File inputFile = new File(
				"../warc-indexer/src/test/resources/gov.uk-revisit-warcs/BL-20140325121225068-00000-32090~opera~8443.warc.gz");
		String archiveName = inputFile.getName();
		
        ArchiveReader reader = ArchiveReaderFactory.get(inputFile);
        Iterator<ArchiveRecord> ir = reader.iterator();
        ArchiveRecord record;
		Text key = new Text();
		WritableArchiveRecord value = new WritableArchiveRecord();
        while( ir.hasNext()) {
			record = (ArchiveRecord) ir.next();
			key.set(archiveName);
			value.setRecord(record);

			LOG.info("GOT: " + record.getHeader().getRecordIdentifier());
			LOG.info("GOT: " + record.getHeader().getMimetype());
			// Skip records that can't be analysed:
			if (skippableRecords.contains(record.getHeader()
					.getMimetype()))
					continue;

			// Run through them all:
				LOG.info("Running without testing output...");
				mapDriver.setInput(key, value);
				List<Pair<Text, Text>> result = mapDriver.run();
				if (result != null && result.size() > 0) {
					MDX mdx = MDX.fromJSONString(result.get(0).getSecond()
							.toString());
					LOG.info("RESULT MDX: " + mdx);

				// Perform a specific check for one of the items:
				if ("http://data.gov.uk/".equals(record.getHeader().getUrl())
						&& record.getHeader().getMimetype()
								.contains("response")) {
					Text testKey = new Text(
							"sha1:SKAVWVVB6HYPSTY3YNQJVM2C4FZRWBSG");
					MDX testMdx = MDX
							.fromJSONString("{\"hash\":\"sha1:SKAVWVVB6HYPSTY3YNQJVM2C4FZRWBSG\",\"url\":\"http://data.gov.uk/\",\"ts\":\"20140325121238\",\"properties\":{\"content_type_norm\":[\"html\"],\"crawl_year\":[\"2014\"],\"source_file_s\":[\"BL-20140325121225068-00000-32090~opera~8443.warc.gz@2475\"],\"ssdeep_hash_ngram_bs_6\":[\"CBRJaug29WHhcRWQHSuSAGW08rJrcUqVNRl8B3tiXWYiOPin\"],\"content_first_bytes\":[\"3c 21 44 4f 43 54 59 50 45 20 68 74 6d 6c 3e 0a 3c 68 74 6d 6c 20 6c 61 6e 67 3d 22 65 6e 22 3e\"],\"links_public_suffixes\":[\"gov.uk\"],\"content_type_droid\":[\"text/html; version=5\"],\"crawl_date\":[\"Tue Mar 25 12:12:38 GMT 2014\"],\"ssdeep_hash_bs_6\":[\"CBRJaug29WHhcRWQHSuSAGW08rJrcUqVNRl8B3tiXWYiOPin\"],\"content_type_served\":[\"text/html; charset=utf-8\"],\"id\":[\"sha1:SKAVWVVB6HYPSTY3YNQJVM2C4FZRWBSG/xyPBet3sIfZO5T/DYf57+Q==\"],\"elements_used\":[\"body\",\"ul\",\"form\",\"link/@rel=shortcut icon\",\"link\",\"a\",\"img\",\"div\",\"script\",\"section\",\"meta\",\"i\",\"footer\",\"li\",\"title\",\"input\",\"link/@rel=stylesheet\",\"button\",\"p\",\"nav\",\"html\",\"head\",\"h2\",\"span\"],\"title\":[\"data.gov.uk\"],\"record_type\":[\"response\"],\"domain\":[\"data.gov.uk\"],\"parse_error\":[\"org.apache.tika.sax.WriteOutContentHandler$WriteLimitReachedException: Your document contained more than 1024 characters, and so your requested limit has been reached. To receive the full text of the\"],\"content_metadata_ss\":[\"viewport=width=device-width, initial-scale=1.0\",\"Tika-Parse-Exception=org.apache.tika.sax.WriteOutContentHandler$WriteLimitReachedException: Your document contained more\",\"X-UA-Compatible=IE=edge,chrome=IE7\",\"Generator=Drupal 7 (http://drupal.org)\",\"X-Parsed-By=org.apache.tika.parser.DefaultParser\"],\"content_type\":[\"text/html\"],\"crawl_years\":[\"{add=2014}\"],\"content_type_version\":[\"5\"],\"wayback_date\":[\"20140325121238\"],\"host\":[\"data.gov.uk\"],\"hash\":[\"sha1:SKAVWVVB6HYPSTY3YNQJVM2C4FZRWBSG\"],\"content_ffb\":[\"3c21444f\"],\"content_type_full\":[\"text/html; charset=utf-8; version=5\"],\"crawl_dates\":[\"{add=2014-03-25T12:12:38Z}\"],\"url\":[\"http://data.gov.uk/\"],\"ssdeep_hash_bs_12\":[\"cXg1aRWQHSuSAGcrJYUuDcIXWJOqn\"],\"ssdeep_hash_ngram_bs_12\":[\"cXg1aRWQHSuSAGcrJYUuDcIXWJOqn\"],\"public_suffix\":[\"gov.uk\"],\"url_type\":[\"slashpage\"],\"content_language\":[\"en\"],\"server\":[\"nginx/1.4.4\",\"PHP/5.3.10-1ubuntu3.10\"],\"content_length\":[\"14786\"],\"content_text_length\":[\"290\"],\"links_domains\":[\"data.gov.uk\"],\"content_encoding\":[\"UTF-8\"],\"links_hosts\":[\"data.gov.uk\"],\"content_type_tika\":[\"text/html; charset=utf-8\"]}}");
					assertEquals(testKey, result.get(0).getFirst());
					assertEquals(testMdx.getUrl(), mdx.getUrl());
					assertEquals(testMdx.getHash(), mdx.getHash());
					assertEquals(testMdx.getTs(), mdx.getTs());
				}

				}
				mapDriver.resetOutput();
		}
	}

}
