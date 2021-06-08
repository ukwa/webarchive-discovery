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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream implementation that wraps a DataOutput.
 */
public class DataOutputOutputStream extends OutputStream {

  private final DataOutput out;

  /**
   * Construct an OutputStream from the given DataOutput. If 'out'
   * is already an OutputStream, simply returns it. Otherwise, wraps
   * it in an OutputStream.
   * @param out the DataOutput to wrap
   * @return an OutputStream instance that outputs to 'out'
   */
  public static OutputStream constructOutputStream(DataOutput out) {
    if (out instanceof OutputStream) {
      return (OutputStream)out;
    } else {
      return new DataOutputOutputStream(out);
    }
  }

  private DataOutputOutputStream(DataOutput out) {
    this.out = out;
  }

  @Override
  public void write(int b) throws IOException {
    out.writeByte(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }
}
