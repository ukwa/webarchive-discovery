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
package uk.bl.wa.analyser.text.lang;

/*
 * #%L
 * warc-indexer
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

import java.io.IOException;
import java.io.Writer;

/**
 * Writer that builds a language profile based on all the written content.
 *
 * @since Apache Tika 0.5
 */
public class ProfilingWriter extends Writer {

    private final LanguageProfile profile;

    private char[] buffer = new char[] { '_', 0, 0 };

    private int n = 1;

    public ProfilingWriter(LanguageProfile profile) {
        this.profile = profile;
    }

    public ProfilingWriter() {
        this(new LanguageProfile());
    }

    /**
     * Returns the language profile being built by this writer. Note that
     * the returned profile gets updated whenever new characters are written.
     * Use the {@link #getLanguage()} method to get the language that best
     * matches the current state of the profile.
     *
     * @return language profile
     */
    public LanguageProfile getProfile() {
        return profile;
    }

    /**
     * Returns the language that best matches the current state of the
     * language profile.
     *
     * @return language that best matches the current profile
     */
    public LanguageIdentifier getLanguage() {
        return new LanguageIdentifier(profile);
    }

    @Override
    public void write(char[] cbuf, int off, int len) {
        for (int i = 0; i < len; i++) {
            char c = Character.toLowerCase(cbuf[off + i]);
            if (Character.isLetter(c)) {
                addLetter(c);
            } else {
                addSeparator();
            }
        }
    }

    private void addLetter(char c) {
        System.arraycopy(buffer, 1, buffer, 0, buffer.length - 1);
        buffer[buffer.length - 1] = c;
        n++;
        if (n >= buffer.length) {
            profile.add(new String(buffer));
        }
    }

    private void addSeparator() {
        addLetter('_');
        n = 1;
    }

    @Override
    public void close() throws IOException {
        addSeparator();
    }

    /**
     * Ignored.
     */
    @Override
    public void flush() {
    }

}
