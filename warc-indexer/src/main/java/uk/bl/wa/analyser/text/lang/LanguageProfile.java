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

    /**
     * Sorted ngram cache for faster distance calculation.
     */
    private Interleaved interleaved = new Interleaved();
    public static boolean useInterleaved = true; // For testing purposes

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
     *
     * @param that the other language profile
     * @return distance between the profiles
     */
    public double distance(LanguageProfile that) {
        return useInterleaved ? distanceInterleaved(that) : distanceStandard(that);
    }

    private double distanceStandard(LanguageProfile that) {
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

        Set<String> ngrams = new HashSet<String>();
        ngrams.addAll(this.ngrams.keySet());
        ngrams.addAll(that.ngrams.keySet());
        Instrument.timeRel("LanguageProfile.distanceStandard#total",
                           "LanguageProfile.distanceStandard#setcreation", start);

        final long istart = System.nanoTime();
        for (String ngram : ngrams) {
            double thisFrequency = this.getCount(ngram) / thisCount;
            double thatFrequency = that.getCount(ngram) / thatCount;
            double difference = thisFrequency - thatFrequency;
            sumOfSquares += difference * difference;
        }

        Instrument.timeRel("LanguageProfile.distanceStandard#total",
                           "LanguageProfile.distanceStandard#dist", istart);
        Instrument.timeRel("LanguageIdentifier#matchlanguageprofile",
                           "LanguageProfile.distanceStandard#total", start);
        return Math.sqrt(sumOfSquares);
    }

    @Override
    public String toString() {
        return ngrams.toString();
    }

    public int getNgramLength() {
        return length;
    }
    //             Instrument.timeRel("LanguageProfile.distance#total", "LanguageProfile.getSortedNGrams", start);
    private double distanceInterleaved(LanguageProfile that) {
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

        Interleaved.Entry thisEntry = updateInterleaved().firstEntry();
        Interleaved.Entry thatEntry = that.updateInterleaved().firstEntry();

        final long istart = System.nanoTime();
        // Iterate the lists in parallel, until both lists has been depleted
        while (thisEntry.hasNgram() || thatEntry.hasNgram()) {
            if (!thisEntry.hasNgram()) { // Depleted this
                sumOfSquares += square(thatEntry.count / thatCount);
                thatEntry.next();
                continue;
            }

            if (!thatEntry.hasNgram()) { // Depleted that
                sumOfSquares += square(thisEntry.count / thisCount);
                thisEntry.next();
                continue;
            }

            final int compare = thisEntry.compareTo(thatEntry);

            if (compare == 0) { // Term exists both in this and that
                double difference = thisEntry.count/thisCount - thatEntry.count/thatCount;
                sumOfSquares += square(difference);
                thisEntry.next();
                thatEntry.next();
            } else if (compare < 0) { // Term exists only in this
                sumOfSquares += square(thisEntry.count/thisCount);
                thisEntry.next();
            } else { // Term exists only in that
                sumOfSquares += square(thatEntry.count/thatCount);
                thatEntry.next();
            }
//            System.out.println(thisEntry + " vs " + thatEntry + " sum: " + sumOfSquares);
        }
        Instrument.timeRel("LanguageProfile.distanceInterleaved#total",
                           "LanguageProfile.distanceInterleaved#dist", istart);
        Instrument.timeRel("LanguageIdentifier#matchlanguageprofile",
                           "LanguageProfile.distanceInterleaved#total", start);
        return Math.sqrt(sumOfSquares);
    }
    private double square(double count) {
        return count * count;
    }

    private class Interleaved {

        private char[] entries = null; // <ngram(length chars)><count(2 chars)>*
        private int size = 0; // Number of entries (one entry = length+2 chars)
        private long entriesGeneratedAtCount = -1; // Keeps track of when the sequential structure was current

        /**
         * Ensure that the entries array is in sync with the ngrams.
         */
        public void update() {
            if (count == entriesGeneratedAtCount) { // Already up to date
                return;
            }
            final long start = System.nanoTime();
            size = ngrams.size();
            final int numChars = (length+2)*size;
            if (entries == null || entries.length < numChars) {
                entries = new char[numChars];
            }
            int pos = 0;
            for (Map.Entry<String, Counter> entry: getSortedNgrams()) {
                for (int l = 0 ; l < length ; l++) {
                    entries[pos + l] = entry.getKey().charAt(l);
                }
                entries[pos + length] = (char)(entry.getValue().count / 65536); // Upper 16 bit
                entries[pos + length + 1] = (char)(entry.getValue().count % 65536); // lower 16 bit
                pos += length + 2;
            }
            entriesGeneratedAtCount = count;
            Instrument.timeRel("LanguageProfile.distanceInterleaved#total", "LanguageProfile.Interleaved.update", start);
        }

        public Entry firstEntry() {
            Entry entry = new Entry();
            if (size > 0) {
                entry.update(0);
            }
            return entry;
        }

        private List<Map.Entry<String, Counter>> getSortedNgrams() {
            List<Map.Entry<String, Counter>> entries = new ArrayList<Map.Entry<String, Counter>>(ngrams.size());
            entries.addAll(ngrams.entrySet());
            Collections.sort(entries, new Comparator<Map.Entry<String, Counter>>() {
                @Override
                public int compare(Map.Entry<String, Counter> o1, Map.Entry<String, Counter> o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
            });
            return entries;
        }

        private class Entry implements Comparable<Entry> {
            char[] ngram = new char[length];
            int count = 0;
            int pos = 0;

            private void update(int pos) {
                this.pos = pos;
                if (pos >= size) { // Reached the end
                    return;
                }
                final int origo = pos*(length+2);
                System.arraycopy(entries, origo, ngram, 0, length);
                count = entries[origo+length] * 65536 + entries[origo+length+1];
            }

            @Override
            public int compareTo(Entry other) {
                for (int i = 0 ; i < ngram.length ; i++) {
                    if (ngram[i] != other.ngram[i]) {
                        return ngram[i] - other.ngram[i];
                    }
                }
                return 0;
            }
            public boolean hasNext() {
                return pos < size-1;
            }
            public boolean hasNgram() {
                return pos < size;
            }
            public void next() {
                update(pos+1);
            }
            public String toString() {
                return new String(ngram) + "(" + count + ")";
            }
        }
    }
    private Interleaved updateInterleaved() {
        interleaved.update();
        return interleaved;
    }

}
