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
package uk.bl.wa.solr;

/*-
 * #%L
 * warc-indexer
 * %%
 * Copyright (C) 2013 - 2023 The webarchive-discovery project contributors
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

import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Field content adjuster used by {@link SolrRecord}.
 */
public class FieldAdjuster implements UnaryOperator<String> {
    private final int maxValues;
    private final Function<String, String> inner;
    private final String pipeline;

    public static final FieldAdjuster PASSTHROUGH = new FieldAdjuster(-1, s -> s);

    public FieldAdjuster(int maxValues, Function<String, String> inner) {
        this(maxValues, inner, "N/A");
    }

    public FieldAdjuster(int maxValues, Function<String, String> inner, String pipelineDescription) {
        this.maxValues = maxValues;
        this.inner = inner;
        this.pipeline = pipelineDescription;
    }

    @Override
    public String apply(String s) {
        return maxValues == 0 ? null : inner.apply(s);
    }

    /**
     * @return the maximum allowed valued for the given field.
     */
    public int getMaxValues() {
        return maxValues;
    }

    @Override
    public String toString() {
        return "FieldAdjuster{" +
               "maxValues=" + maxValues +
               ", pipeline=[" + pipeline +
               "]}";
    }
}
