package uk.bl.wa.util;

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

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URL;

import static org.junit.Assert.*;

/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
public class ValidateWARCNameMatchersTest {

    // Not a proper test as the user must inspect the output
    @Test
    public void testBasics() throws IOException {
        final URL CONFIG = Thread.currentThread().getContextClassLoader().getResource("arcnameanalyser.conf");
        final String WARCS =
                "/netarkiv/0101/filedir/15626-38-20070418024637-00385-sb-prod-har-001.statsbiblioteket.dk.arc\n" +
                "25666-33-20080221003533-00046-sb-prod-har-004.arc";
        ValidateWARCNameMatchers.validateRules(
                ValidateWARCNameMatchers.getRules(CONFIG.getPath()),
                new BufferedReader(new StringReader(WARCS)),
                true);
    }
}
