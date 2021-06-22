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
