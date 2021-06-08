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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.FastOutputStream;
import org.apache.solr.common.util.JavaBinCodec;

public class SolrInputDocumentWritable implements Writable {
  private SolrInputDocument sid;

  public SolrInputDocumentWritable() {
  }

  public SolrInputDocumentWritable(SolrInputDocument sid) {
    this.sid = sid;
  }

  public SolrInputDocument getSolrInputDocument() {
    return sid;
  }

  @Override
  public String toString() {
    return sid.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();
    FastOutputStream daos = FastOutputStream.wrap(DataOutputOutputStream.constructOutputStream(out));
    codec.init(daos);
    try {
      codec.writeVal(sid);
    } finally {
      daos.flushBuffer();
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    JavaBinCodec codec = new JavaBinCodec();
    UnbufferedDataInputInputStream dis = new UnbufferedDataInputInputStream(in);
    sid = (SolrInputDocument)codec.readVal(dis);
  }

}
