/**
 * 
 */
package uk.bl.wa.apache.solr.hadoop;

/*
 * #%L
 * warc-hadoop-indexer
 * %%
 * Copyright (C) 2013 - 2021 The webarchive-discovery project contributors
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

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

}
