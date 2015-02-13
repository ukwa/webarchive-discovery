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
 * Copyright (C) 2013 - 2015 The UK Web Archive
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

import uk.bl.wa.util.Instrument;

import java.util.*;

/*
Performance improved at the cost of more permanent storage space as opposed to temporary storage spage.
 */
/**
 * Language profile based on ngram counts.
 *
 * @since Apache Tika 0.5
 */
public class LanguageProfile {

    public static final int DEFAULT_NGRAM_LENGTH = 3;

    private final int length;

    /**
     * The ngrams that make up this profile.
     */
    private final Map<String, Counter> ngrams = new HashMap<String, Counter>();
    private final List<Map.Entry<String, Counter>> ngrams_sorted = new ArrayList<Map.Entry<String, Counter>>();
    private long sortedLastUpdatedCount = 0;

    /**
     * The sum of all ngram counts in this profile.
     * Used to calculate relative ngram frequency.
     */
    private long count = 0;

    private static class Counter {
        private long count = 0;
        public String toString() {
            return Long.toString(count);
        }
    }

    public LanguageProfile(int length) {
        this.length = length;
    }

    public LanguageProfile() {
        this(DEFAULT_NGRAM_LENGTH);
    }

    public LanguageProfile(String content, int length) {
        this(length);
        final long start = System.nanoTime();
        ProfilingWriter writer = new ProfilingWriter(this);
        char[] ch = content.toCharArray();
        writer.write(ch, 0, ch.length);
        Instrument.timeRel("LanguageDetector.detectLanguage#li", "LanguageProfile#profilewriter", start);
    }

    public LanguageProfile(String content) {
        this(content, DEFAULT_NGRAM_LENGTH);
    }

    public long getCount() {
        return count;
    }

    public long getCount(String ngram) {
        Counter counter = ngrams.get(ngram);
        if (counter != null) {
            return counter.count;
        } else {
            return 0;
        }
    }

    /**
     * Adds a single occurrence of the given ngram to this profile.
     *
     * @param ngram the ngram
     */
    public void add(String ngram) {
        add(ngram, 1);
    }

    /**
     * Adds multiple occurrences of the given ngram to this profile.
     *
     * @param ngram the ngram
     * @param count number of occurrences to add
     */
    public void add(String ngram, long count) {
        if (length != ngram.length()) {
            throw new IllegalArgumentException(
                    "Unable to add an ngram of incorrect length: "
                    + ngram.length() + " != " + length);
        }

        Counter counter = ngrams.get(ngram);
        if (counter == null) {
            counter = new Counter();
            ngrams.put(ngram, counter);
        }
        counter.count += count;
        this.count += count;
    }

    /**
     * Calculates the geometric distance between this and the given
     * other language profile.
     * </p><p>
     * This implementations makes parallel runs through the dataset for this and that.
     * This is faster and with less memory and GC-overhead than the old implementation {@link #distanceOld}.
     * @param that the other language profile
     * @return distance between the profiles
     */
    public double distance(LanguageProfile that) {
        final long start = System.nanoTime();
        if (length != that.length) {
            throw new IllegalArgumentException(
                    "Unable to calculage distance of language profiles"
                    + " with different ngram lengths: "
                    + that.length + " != " + length);
        }

        double sumOfSquares = 0.0;
        double thisCount = Math.max(this.count, 1.0);
        double thatCount = Math.max(that.count, 1.0);

        int thisPos = 0;
        int thatPos = 0;
        List<Map.Entry<String, Counter>> thisList = getSortedNgrams();
        List<Map.Entry<String, Counter>> thatList = that.getSortedNgrams();

        // Iterate the lists in parallel, until both lists has been depleted
        while (thisPos < thisList.size() || thatPos < thatList.size()) {

            if (thisPos == thisList.size()) { // Depleted this
                sumOfSquares += square(thatList.get(thatPos++).getValue().count / thatCount);
                continue;
            }

            if (thatPos == thatList.size()) { // Depleted that
                sumOfSquares += square(thisList.get(thisPos++).getValue().count / thisCount);
                continue;
            }

            final String thisVal = thisList.get(thisPos).getKey();
            final String thatVal = thatList.get(thatPos).getKey();
            final int compare = thisVal.compareTo(thatVal);

            if (compare == 0) { // Term exists both in this and that
                double difference = (thisList.get(thisPos++).getValue().count / thisCount) -
                                    (thatList.get(thatPos++).getValue().count / thisCount);
                sumOfSquares += square(difference);
            } else if (compare < 0) { // Term exists only in this
                sumOfSquares += square(thisList.get(thisPos++).getValue().count / thisCount);
            } else { // Term exists only in that
                sumOfSquares += square(thatList.get(thatPos++).getValue().count / thatCount);
            }
        }
        Instrument.timeRel("LanguageIdentifier#matchlanguageprofile", "LanguageProfile.distance#total", start);
        return Math.sqrt(sumOfSquares);
    }

    private double square(double count) {
        return count * count;
    }

    /**
     * Calculates the geometric distance between this and the given
     * other language profile.
     *
     * @param that the other language profile
     * @return distance between the profiles
     */
    public double distanceOld(LanguageProfile that) {
        if (length != that.length) {
            throw new IllegalArgumentException(
                    "Unable to calculage distance of language profiles"
                    + " with different ngram lengths: "
                    + that.length + " != " + length);
        }

        double sumOfSquares = 0.0;
        double thisCount = Math.max(this.count, 1.0);
        double thatCount = Math.max(that.count, 1.0);

        Set<String> ngrams = new HashSet<String>();
        ngrams.addAll(this.ngrams.keySet());
        ngrams.addAll(that.ngrams.keySet());
        for (String ngram : ngrams) {
            double thisFrequency = this.getCount(ngram) / thisCount;
            double thatFrequency = that.getCount(ngram) / thatCount;
            double difference = thisFrequency - thatFrequency;
            sumOfSquares += difference * difference;
        }

        return Math.sqrt(sumOfSquares);
    }

    @Override
    public String toString() {
        return ngrams.toString();
    }

    public int getNgramLength() {
        return length;
    }

    public List<Map.Entry<String, Counter>> getSortedNgrams() {
        if (sortedLastUpdatedCount != count) {
            final long start = System.nanoTime();
            ngrams_sorted.clear();
            ngrams_sorted.addAll(ngrams.entrySet());
            Collections.sort(ngrams_sorted, new Comparator<Map.Entry<String, Counter>>() {
                @Override
                public int compare(Map.Entry<String, Counter> o1, Map.Entry<String, Counter> o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
            });
            sortedLastUpdatedCount = count;
            Instrument.timeRel("LanguageProfile.distance#total", "LanguageProfile.getSortedNGrams", start);
        }
        return ngrams_sorted;
    }
}
