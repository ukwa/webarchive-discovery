/**
 * 
 */
package org.apache.solr.hadoop;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.zookeeper.KeeperException;

import com.google.common.io.Files;

public class Zipper {

	public static void zipDir(File dirName, File nameZipFile)
			throws IOException {
		ZipOutputStream zip = null;
		FileOutputStream fW = null;
		fW = new FileOutputStream(nameZipFile);
		zip = new ZipOutputStream(fW);
		for (File f : dirName.listFiles()) {
			if (f.isDirectory()) {
				addFolderToZip("", f, zip);
			} else {
				addFileToZip("", f, zip, false);
			}
		}
		zip.close();
		fW.close();
	}

	private static void addFolderToZip(String path, File srcFolder,
			ZipOutputStream zip) throws IOException {
		if (srcFolder.list().length == 0) {
			addFileToZip(path, srcFolder, zip, true);
		} else {
			for (File f : srcFolder.listFiles()) {
				if (path.equals("")) {
					addFileToZip(srcFolder.getName(), f,
							zip, false);
				} else {
					addFileToZip(path + "/" + srcFolder.getName(), f, zip,
							false);
				}
			}
		}
	}

	private static void addFileToZip(String path, File folder,
			ZipOutputStream zip,
			boolean flag) throws IOException {
		if (flag) {
			zip.putNextEntry(new ZipEntry(path + "/" + folder.getName() + "/"));
		} else {
			if (folder.isDirectory()) {
				addFolderToZip(path, folder, zip);
			} else {
				byte[] buf = new byte[1024];
				int len;
				FileInputStream in = new FileInputStream(folder);
				zip.putNextEntry(new ZipEntry(path + "/" + folder.getName()));
				while ((len = in.read(buf)) > 0) {
					zip.write(buf, 0, len);
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, KeeperException,
			InterruptedException {
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

		//
		Zipper.zipDir(new File("src/test"), new File("target/temp.zip"));
	}
}
