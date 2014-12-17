package org.apache.solr.hadoop;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import uk.bl.wa.apache.solr.hadoop.Solate;
import uk.bl.wa.apache.solr.hadoop.Zipper;
import uk.bl.wa.apache.solr.hadoop.ZooKeeperInspector;

import com.google.common.io.Files;

public class SolateTest {
	private static final Log log = LogFactory.getLog(SolateTest.class);

	// Test cluster:
	private MiniDFSCluster dfsCluster = null;
	private Configuration conf;

	// @ Before
	public void setUp() throws Exception {
		conf = new Configuration();
		conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		dfsCluster = new MiniDFSCluster(conf, 1, true, null);
		dfsCluster.getFileSystem().setConf(conf);
	}

	// @ After
	public void tearDown() throws Exception {
		log.warn("Tearing down test cluster...");
		if (dfsCluster != null) {
			dfsCluster.shutdown();
			dfsCluster = null;
		}
	}

	@Test
	public void testDummyTest() {
	}

	public void testDownloadSolrHomeFromZooKeeper() throws Exception,
			InterruptedException, KeeperException {
		String zkHost = "openstack2.ad.bl.uk:2181,openstack4.ad.bl.uk:2181,openstack5.ad.bl.uk:2181/solr";
		String collection = "jisc2";
		String solrHomeZipName = "cloud-config.zip";

		ZooKeeperInspector zki = new ZooKeeperInspector();
		org.apache.solr.common.cloud.SolrZkClient zkClient = zki
				.getZkClient(zkHost);
		String configName = zki.readConfigName(zkClient, collection);
		File tmpSolrHomeDir = zki.downloadConfigDir(zkClient, configName);

		// Create a ZIP file:
		File solrHomeLocalZip = File.createTempFile("tmp-", solrHomeZipName);
		solrHomeLocalZip.deleteOnExit();
		Zipper.zipDir(tmpSolrHomeDir, solrHomeLocalZip);
		System.out.println("Written to " + solrHomeLocalZip);
		Files.copy(solrHomeLocalZip, new File("target/" + solrHomeZipName));
		System.out.println("Written to " + solrHomeZipName);

	}

	public static void listFiles(Path path, FileSystem fs) throws IOException {
		for (FileStatus s : fs.listStatus(path)) {
			log.info("listFiles looking at " + s.getPath());
			if (s.isDir()) {
				listFiles(s.getPath(), fs);
			}
		}
	}

	// @ Test
	// Test disabled as standard Solr does not support older Hadoop versions
	public void testCreateEmbeddedSolrServerPathFileSystemPathPath()
			throws Exception {
		File tmpSolrHomeDir = new File("../warc-indexer/src/main/solr/solr/")
				.getAbsoluteFile();
		
		Path outputShardDir = new Path("/").makeQualified(this.dfsCluster
				.getFileSystem());

		// Try to make a core:
		EmbeddedSolrServer solr = Solate.createEmbeddedSolrServer(new Path(
				tmpSolrHomeDir.toString()), this.dfsCluster.getFileSystem(),
				new Path("output"),
				outputShardDir);
		SolrInputDocument in = new SolrInputDocument();
		in.addField("id", "1");
		in.addField("title", "titele");
		solr.add(in);
		solr.commit();

		SolrParams sp = new SolrQuery("*:*");
		QueryResponse r = solr.query(sp);
		log.info("QR: " + r);
		assertEquals("Query failed!", 1l, r.getResults().getNumFound());

		listFiles(new Path("/"), this.dfsCluster.getFileSystem());

	}
}
