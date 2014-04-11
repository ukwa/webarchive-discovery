package org.apache.solr.hadoop;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
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

	private final Path input = new Path("/input");
	private final Path output = new Path("/data");

	@Before
	public void setUp() throws Exception {
		conf = new Configuration();
		dfsCluster = new MiniDFSCluster(conf, 1, true, null);
		dfsCluster.getFileSystem().makeQualified(input);
		dfsCluster.getFileSystem().makeQualified(output);
	}

	@After
	public void tearDown() throws Exception {
		log.warn("Tearing down test cluster...");
		if (dfsCluster != null) {
			dfsCluster.shutdown();
			dfsCluster = null;
		}
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

	@Test
	public void testCreateEmbeddedSolrServerPathFileSystemPathPath()
			throws Exception {
		File tmpSolrHomeDir = new File("../warc-indexer/src/main/solr/solr/")
				.getAbsoluteFile();
		
		Path outputShardDir = this.dfsCluster.getFileSystem()
				.getWorkingDirectory();
		Path hdfsDirPath = new Path(this.dfsCluster.getFileSystem()
				.getWorkingDirectory(), "/data");

		FileSystem fileSystem = FileSystem.get(
				new Path("/data").toUri(), conf);

		if (!fileSystem.exists(hdfsDirPath)) {
			log.error("No " + hdfsDirPath);
			boolean success = fileSystem.mkdirs(hdfsDirPath);
			if (!success) {
				log.error("Failed! " + hdfsDirPath);
				return;
			} else {
				log.error("Created " + hdfsDirPath);
			}
		}
		/*
		 * HdfsDirectory dir = new HdfsDirectory(hdfsDirPath, conf);
		 * log.warn("Got dir: " + dir.getHdfsDirPath());
		 */

		// Try to make a core:
		Solate.createEmbeddedSolrServer(new Path(tmpSolrHomeDir.toString()),
				this.dfsCluster.getFileSystem(), new Path("output"),
				outputShardDir);

	}

}
