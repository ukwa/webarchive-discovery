/**
 * 
 */
package org.apache.solr.hadoop;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class Solate {
	private static final Logger LOG = LoggerFactory.getLogger(Solate.class);
	private int shards;
	private HashMap<String, Integer> shardNumbers;
	private DocCollection docCollection;
	private final SolrParams emptySolrParams = new MapSolrParams(
			Collections.<String, String> emptyMap());

	public Solate(String zkHost, String collection, int numShards) {
		this.shards = numShards;
		if (shards <= 0) {
			throw new IllegalArgumentException("Illegal shards: " + shards);
		}
		if (zkHost == null) {
			throw new IllegalArgumentException("zkHost must not be null");
		}
		if (collection == null) {
			throw new IllegalArgumentException("collection must not be null");
		}
		LOG.info("Using SolrCloud zkHost: {}, collection: {}", zkHost,
				collection);
		docCollection = new ZooKeeperInspector()
				.extractDocCollection(zkHost,
				collection);
		if (docCollection == null) {
			throw new IllegalArgumentException("docCollection must not be null");
		}
		if (docCollection.getSlicesMap().size() != shards) {
			throw new IllegalArgumentException("Incompatible shards: + "
					+ shards + " for docCollection: " + docCollection);
		}
		LOG.info("Got slices: " + docCollection.getSlices().size());
		for (Slice s : docCollection.getSlices()) {
			LOG.info("Slice: " + s.getName());
		}
		List<Slice> slices = new ZooKeeperInspector()
				.getSortedSlices(docCollection.getSlices());
		if (slices.size() != shards) {
			throw new IllegalStateException("Incompatible sorted shards: + "
					+ shards + " for docCollection: " + docCollection);
		}
		shardNumbers = new HashMap<String, Integer>(slices.size());

		for (int i = 0; i < slices.size(); i++) {
			shardNumbers.put(slices.get(i).getName(), i);
		}
		LOG.debug("Using SolrCloud docCollection: {}", docCollection);
		DocRouter docRouter = docCollection.getRouter();
		if (docRouter == null) {
			throw new IllegalArgumentException("docRouter must not be null");
		}
		LOG.info("Using SolrCloud docRouterClass: {}", docRouter.getClass());
	}

	public int getPartition(String keyStr, SolrInputDocument doc) {
		DocRouter docRouter = docCollection.getRouter();

		Slice slice = docRouter.getTargetSlice(keyStr, doc, emptySolrParams,
				docCollection);

		if (slice == null) {
			throw new IllegalStateException(
					"No matching slice found! The slice seems unavailable. docRouterClass: "
							+ docRouter.getClass().getName());
		}

		int rootShard = shardNumbers.get(slice.getName());
		if (rootShard < 0 || rootShard >= shards) {
			throw new IllegalStateException("Illegal shard number " + rootShard
					+ " for slice: " + slice + ", docCollection: "
					+ docCollection);
		}

		LOG.debug("Slice " + slice.getName() + " == #" + rootShard);

	    return rootShard;
	}

	/**
	 * 
	 * @param conf
	 * @param zipName
	 * @return
	 * @throws IOException
	 */
	public static Path findSolrConfig(JobConf conf, String zipName)
			throws IOException {
		Path solrHome = null;
		Path[] localArchives = DistributedCache.getLocalCacheArchives(conf);
		if (localArchives.length == 0) {
			LOG.error("No local cache archives.");
			throw new IOException(String.format("No local cache archives."));
		}
		for (Path unpackedDir : localArchives) {
			LOG.info("Looking at: " + unpackedDir + " for " + zipName);
			if (unpackedDir.getName().equals(zipName)) {
				LOG.info("Using this unpacked directory as solr home: {}",
						unpackedDir);
				solrHome = unpackedDir;
				break;
			}
		}
		return solrHome;
	}

	/**
	 * 
	 * @param solrHomeDir
	 * @param fs
	 * @param outputDir
	 * @param outputShardDir
	 * @return
	 * @throws IOException
	 */
	public static EmbeddedSolrServer createEmbeddedSolrServer(Path solrHomeDir,
			FileSystem fs, Path outputDir, Path outputShardDir)
			throws IOException {

		if (solrHomeDir == null) {
			throw new IOException("Unable to find solr home setting");
		}
		LOG.info("Creating embedded Solr server with solrHomeDir: "
				+ solrHomeDir + ", fs: " + fs + ", outputShardDir: "
				+ outputShardDir);

		Properties props = new Properties();
		// FIXME note this is odd (no scheme) given Solr doesn't currently
		// support uris (just abs/relative path)
		Path solrDataDir = new Path(outputShardDir, "data");
		if (!fs.exists(solrDataDir) && !fs.mkdirs(solrDataDir)) {
			throw new IOException("Unable to create " + solrDataDir);
		}

		String dataDirStr = solrDataDir.toUri().toString();
		LOG.info("Attempting to set data dir to:" + dataDirStr);
		props.setProperty("solr.data.dir", dataDirStr);
		props.setProperty("solr.home", solrHomeDir.toString());
		props.setProperty("solr.solr.home", solrHomeDir.toString());
		System.setProperty("solr.hdfs.home", outputDir.toString());
		System.setProperty("solr.directoryFactory",
				HdfsDirectoryFactory.class.getName());
		System.setProperty("solr.lock.type", "hdfs");

		SolrResourceLoader loader = new SolrResourceLoader(
				solrHomeDir.toString(), null, props);

		LOG.info(String
				.format("Constructed instance information solr.home %s (%s), instance dir %s, conf dir %s, writing index to solr.data.dir %s, with permdir %s",
						solrHomeDir, solrHomeDir.toUri(),
						loader.getInstanceDir(), loader.getConfigDir(),
						dataDirStr, outputShardDir));

		CoreContainer container = new CoreContainer(loader);
		container.load();
		LOG.error("Setting up core1 descriptor...");
		CoreDescriptor descr = new CoreDescriptor(container, "core1", new Path(
				solrHomeDir, "collection1").toString(), props);

		LOG.error("Creating core1... " + descr.getConfigName());
		SolrCore core = container.create(descr);
		LOG.error("Registering core1...");
		container.register(core, false);

		System.setProperty("solr.hdfs.nrtcachingdirectory", "false");
		System.setProperty("solr.hdfs.blockcache.enabled", "false");
		System.setProperty("solr.autoCommit.maxTime", "-1");
		System.setProperty("solr.autoSoftCommit.maxTime", "-1");
		EmbeddedSolrServer solr = new EmbeddedSolrServer(container, "core1");

		return solr;
	}

	public static EmbeddedSolrServer createEmbeddedSolrServer(Path solrHomeDir,
			FileSystem fs, Path outputShardDir) throws IOException {

		if (solrHomeDir == null) {
			throw new IOException("Unable to find solr home setting");
		}
		LOG.info("Creating embedded Solr server with solrHomeDir: "
				+ solrHomeDir + ", fs: " + fs + ", outputShardDir: "
				+ outputShardDir);

		Path solrDataDir = new Path(outputShardDir, "data");

		String dataDirStr = solrDataDir.toUri().toString();

		Properties props = new Properties();
		props.setProperty(CoreDescriptor.CORE_DATADIR, dataDirStr);

		SolrResourceLoader loader = new SolrResourceLoader(
				solrHomeDir.toString(), null, props);

		LOG.info(String
				.format(Locale.ENGLISH,
						"Constructed instance information solr.home %s (%s), instance dir %s, conf dir %s, writing index to solr.data.dir %s, with permdir %s",
						solrHomeDir, solrHomeDir.toUri(),
						loader.getInstanceDir(), loader.getConfigDir(),
						dataDirStr, outputShardDir));

		// TODO: This is fragile and should be well documented
		System.setProperty("solr.directoryFactory",
				HdfsDirectoryFactory.class.getName());
		System.setProperty("solr.lock.type", "hdfs");
		System.setProperty("solr.hdfs.home", outputShardDir.getParent().toString());
		System.setProperty("solr.hdfs.nrtcachingdirectory", "false");
		System.setProperty("solr.hdfs.blockcache.enabled", "false");
		System.setProperty("solr.autoCommit.maxTime", "600000");
		System.setProperty("solr.autoSoftCommit.maxTime", "-1");

		LOG.info("Instanciating container...");
		CoreContainer container = new CoreContainer(loader);
		LOG.info("Loading container...");
		container.load();

		LOG.info("Creating core descriptor...");
		CoreDescriptor descr = new CoreDescriptor(container, "core1", new Path(
				solrHomeDir, "collection1").toString(), props);

		LOG.info("Creating core...");
		SolrCore core = container.create(descr);

		if (!(core.getDirectoryFactory() instanceof HdfsDirectoryFactory)) {
			throw new UnsupportedOperationException(
					"Invalid configuration. Currently, the only DirectoryFactory supported is "
							+ HdfsDirectoryFactory.class.getSimpleName());
		}

		LOG.info("Registering core...");
		container.register(core, false);

		LOG.info("Returning EmbeddedSolrServer...");
		EmbeddedSolrServer solr = new EmbeddedSolrServer(container, "core1");
		return solr;
	}

	public static void cacheSolrHome(JobConf conf, String zkHost,
			String collection, String solrHomeZipName) throws KeeperException,
			InterruptedException, IOException {
		// use the config that this collection uses for the SolrHomeCache.
		ZooKeeperInspector zki = new ZooKeeperInspector();
		org.apache.solr.common.cloud.SolrZkClient zkClient = zki
				.getZkClient(zkHost);
		String configName = zki.readConfigName(zkClient, collection);
		File tmpSolrHomeDir = zki.downloadConfigDir(zkClient, configName);

		// Create a ZIP file:
		File solrHomeLocalZip = File.createTempFile("tmp-", solrHomeZipName);
		Zipper.zipDir(tmpSolrHomeDir, solrHomeLocalZip);

		// Add to HDFS:
		FileSystem fs = FileSystem.get(conf);
		String hdfsSolrHomeDir = fs.getHomeDirectory() + "/solr/tempHome/"
				+ solrHomeZipName;
		fs.copyFromLocalFile(new Path(solrHomeLocalZip.toString()), new Path(
				hdfsSolrHomeDir));

		final URI baseZipUrl = fs.getUri().resolve(
				hdfsSolrHomeDir + '#' + solrHomeZipName);

		// Cache it:
		DistributedCache.addCacheArchive(baseZipUrl, conf);
	}
}
