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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper class for lightweight performance-measuring instrumentation of the code base.
 * By nature the class is shared by the whole JVM. As a consequence it it Thread safe.
 * </p><p>
 * Usage: Call {@code Instrument.time(customid, nanotime)} at relevant places in the code.
 * </p><p>
 * Note: The overhead of calling methods on this class is kept to a minimum.
 * The only methods with non-trivial overheads are {@link #log(boolean)} and {@link #getStats()}.
 * @author Toke Eskildsen <te@statsbiblioteket.dk>
 */
public class Instrument {
    private static Logger log = LoggerFactory.getLogger(Instrument.class);

    public enum SORT {insert, id, time, count, avgtime} // count, time & avgtime are max -> min
    
    private static final Map<String, Stats> trackers = new LinkedHashMap<>();
    private static final long classStart = System.nanoTime();

    /**
     * Init must be called as early as possible as this triggers class-load and thus the setting of {@link #classStart}
     * which is used for calculating relative time usage throughout the project.
     * Multiple calls have no effect.
     */
    public static void init() {
        log.debug("Initialized with classStart " + classStart + "ns and init call " + System.nanoTime() + "ns");
    }
    /**
     * Increment the total time for the tracker with the given ID.
     * The delta is calculated with {@code System.nanotime() - nanoStart}.
     * @param id        id for a tracker. If it does not exist, it will be created.
     * @param nanoStart the start time of the measurement.
     */
    public static void timeRel(String id, long nanoStart) {
        time(null, id, System.nanoTime() - nanoStart);
    }

    /**
     * Increment the total time for the tracker with the given ID.
     * The delta is calculated with {@code System.nanotime() - nanoStart}.
     * @param parent       provides hierarchical output. If the parent does not exist, it will be created.
     * @param id        id for a tracker. If it does not exist, it will be created.
     * @param nanoStart the start time of the measurement.
     */
    public static void timeRel(String parent, String id, long nanoStart) {
        time(parent, id, System.nanoTime() - nanoStart);
    }

    /**
     * Increment the total time for the tracker with the given ID.
     * Synchronized to handle creation of new Stats.
     * @param id           id for a tracker. If it does not exist, it will be created.
     * @param nanoAbsolute the amount of nanoseconds to add.
     */
    public static synchronized void time(String id, long nanoAbsolute) {
        time(null, id, nanoAbsolute);
    }

    public static synchronized void createSortedStat(String id, SORT sort, int maxReturnedChildren) {
        Stats stats = trackers.get(id);
        if (stats == null) {
            stats = new Stats(id, sort, maxReturnedChildren);
            trackers.put(id, stats);
            return;
        }
        if (stats.sort != sort || stats.maxReturnedChildren != maxReturnedChildren) {
            throw new IllegalStateException(
                    "The stat '" + id + "' already exists with different sort/max parameters");
        }
    }

    /**
     * Increment the total time for the tracker with the given ID.
     * Synchronized to handle creation of new Stats.
     * @param parent       provides hierarchical output. If the parent does not exist, it will be created.
     * @param id           id for a tracker. If it does not exist, it will be created.
     * @param nanoAbsolute the amount of nanoseconds to add.
     */
    public static synchronized void time(String parent, String id, long nanoAbsolute) {
        getCreateStats(parent, id).time(nanoAbsolute);
    }

    private static Stats getCreateStats(String parent, String id) {
        Stats stats = trackers.get(id);
        if (stats == null) {
            stats = new Stats(id);
            trackers.put(id, stats);
        }

        // Check parent connection
        if (parent == null) {
            return stats;
        }

        Stats pStats = trackers.get(parent);
        if (pStats == null) {
            pStats = new Stats(parent);
            trackers.put(parent, pStats);
        }
        pStats.addChild(stats);
        return stats;
    }

    /**
     * Sets the time and count for the given tracker to the specified values, regardless of previous content.
     * Synchronized to handle creation of new Stats.
     * @param parent       provides hierarchical output. If the parent does not exist, it will be created.
     * @param id           id for a tracker. If it does not exist, it will be created.
     * @param nano         the amount of nanoseconds to set.
     * @param count        the count to set.
     */
    public static synchronized void setAbsolute(String parent, String id, long nano, long count) {
        getCreateStats(parent, id).setAbsolute(nano, count);
    }

    @SuppressWarnings("WeakerAccess")
    public static class Stats {

        public final SORT sort;
        public final int maxReturnedChildren;

        public final String id;
        public final AtomicLong time = new AtomicLong(0);
        public final AtomicLong count = new AtomicLong(0);
        private final Set<Stats> children = new LinkedHashSet<Stats>();
        private static final double MD = 1000000d;

        public Stats(String id) {
            this(id, SORT.insert, Integer.MAX_VALUE);
        }

        public Stats(String id, SORT sort, int maxReturnedChildren) {
            this.id = id;
            this.sort = sort;
            this.maxReturnedChildren = maxReturnedChildren;
        }

        public void time(long nanoAbsolute) {
            time.addAndGet(nanoAbsolute);
            count.incrementAndGet();
        }

        public void setAbsolute(long nano, long count) {
            this.time.set(nano);
            this.count.set(count);
        }

        public String toString() {
            // % is only correct for single-threaded processing
            return String.format("%s(#=%d, time=%.2fms, avg=%.2f#/ms %.2fms/#, %.2f%%)%s",
                                 id, count.get(), time.get()/MD,
                                 avgCountPerMS(),
                                 avgMS(),
                                 time.get() * 100.0 / (System.nanoTime()-classStart),
                                 sort == SORT.insert ? "" : " top " + maxReturnedChildren + " sort=" + sort);
        }

        private double avgCountPerMS() {
            return time.get() == 0 ? 0 : count.get() / (time.get() / MD);
        }

        private double avgMS() {
            return count.get() == 0 ? 0 : time.get() / MD / count.get();
        }

        public void addChild(Stats child) {
            children.add(child);
        }

        /**
         * If {@link #sort} is not {@link SORT#insert}, a sort will be performed on each call to this method.
         */
        public Collection<Stats> getChildren() {
            if (sort == SORT.insert && children.size() <= maxReturnedChildren) {
                return children;
            }
            List<Stats> sorted = new ArrayList<>(children);
            if (sort != SORT.insert) {
                Collections.sort(sorted, new Comparator<Stats>() {
                    @Override
                    public int compare(Stats o1, Stats o2) {
                        switch (sort) {
                            case id:
                                return o1.id.compareTo(o2.id);
                            case count:
                                return Long.compare(o2.count.get(), o1.count.get());
                            case time:
                                return Long.compare(o2.time.get(), o1.time.get());
                            case avgtime:
                                return Double.compare(o2.avgMS(), o1.avgMS());
                            default:
                                throw new IllegalStateException("Unhandled sort " + sort);
                        }
                    }
                });
            }
            if (sorted.size() > maxReturnedChildren) {
                sorted = sorted.subList(0, maxReturnedChildren);
            }
            return sorted;
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return obj instanceof Stats && id.equals(((Stats) obj).id);
        }

    }

    /**
     * Logs collected statistics.
     * @param major if true, the report is logged at INFO level. If false, DEBUG.
     */
    public static void log(boolean major) {
        if (major) {
            log.info("Performance statistics\n" + getStats());
        } else {
            log.debug("Performance statistics\n" + getStats());
        }
    }

    public static String getStats() {
        // Locate all root stats (the ones that are not children)
        Set<Stats> topLevel = new HashSet<Stats>(trackers.values());
        for (Stats stats: trackers.values()) {
            for (Stats child: stats.children) {
                topLevel.remove(child);
            }
        }

        // Collect recursive stats from all root elements
        StringBuilder sb = new StringBuilder();
        for (Stats stats: topLevel) {
            getStatsRecursive(stats, sb, "");
        }
        return sb.toString();
    }

    private static void getStatsRecursive(Stats stats, StringBuilder sb, String indent) {
        if (sb.length() != 0) {
            sb.append("\n");
        }
        sb.append(indent).append(stats);
        for (Stats child: stats.getChildren()) {
            getStatsRecursive(child, sb, indent + "  ");
        }
    }

    /**
     * Adds a hook to the JVM listening for shutdown and logging when it happens.
     */
    public static void addLoggingShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new CustomShutdownHook("Shutdown stats logging"));
    }
    private static class CustomShutdownHook extends Thread {
        private CustomShutdownHook(String name) {
            super(name);
        }

        @Override
        public void run(){
            try {
                log.info("JVM shutdown detected, dumping collected performance statistics");
                Instrument.log(true);
            } catch (NullPointerException ne) {
                System.err.println("Unable to properly log performance statistics as the logging system has shut down");
                System.err.println(Instrument.getStats());
            }

        }
    }

    // Unreliable (classStart is not cleared), so package private. Used for test only
    static void clear() {
        trackers.clear();
    }
}
