package org.apache.solr.util;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class HdfsUtil {
  
  private static final String[] HADOOP_CONF_FILES = {"core-site.xml",
    "hdfs-site.xml", "mapred-site.xml", "yarn-site.xml", "hadoop-site.xml"};
  
  public static void addHdfsResources(Configuration conf, String confDir) {
  // Hack in some configuration here as we don't necessarily have a HdfsDirectoryFactory in solrconfig.xml
  // This is required in order for FileSystem.get to be thread-safe:
  conf.setBoolean("fs.hdfs.impl.disable.cache", true);
  conf.setBoolean("solr.hdfs.nrtcachingdirectory", false);
  conf.setBoolean("solr.hdfs.blockcache.enabled", false);
  // Avoiding DMA here: THIS DOES NOTHING:
  //conf.setBoolean("solr.hdfs.blockcache.direct.memory.allocation",false);
  /*
   * Caused by: java.lang.RuntimeException: The max direct memory is likely too low.  
   * Either increase it (by adding -XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages to your 
   * containers startup args) or disable direct allocation using solr.hdfs.blockcache.direct.memory.allocation=false 
   * in solrconfig.xml. If you are putting the block cache on the heap, your java heap size might not be large 
   * enough. Failed allocating ~134.217728 MB.
   */
  if (confDir != null && confDir.length() > 0) {
    File confDirFile = new File(confDir);
    if (!confDirFile.exists()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Resource directory does not exist: " + confDirFile.getAbsolutePath());
    }
    if (!confDirFile.isDirectory()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Specified resource directory is not a directory" + confDirFile.getAbsolutePath());
    }
    if (!confDirFile.canRead()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Resource directory must be readable by the Solr process: " + confDirFile.getAbsolutePath());
    }
    for (String file : HADOOP_CONF_FILES) {
      if (new File(confDirFile, file).exists()) {
        conf.addResource(new Path(confDir, file));
      }
    }
  }
  }
}
