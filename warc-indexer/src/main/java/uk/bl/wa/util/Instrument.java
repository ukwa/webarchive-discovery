package uk.bl.wa.util;

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

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.zookeeper.data.Stat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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
    private static Log log = LogFactory.getLog(Instrument.class);

    private static final Map<String, Stats> trackers = new HashMap<String, Stats>();
    private static final long classStart = System.nanoTime();

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

    /**
     * Increment the total time for the tracker with the given ID.
     * Synchronized to handle creation of new Stats.
     * @param parent       provides hierarchical output. If the parent does not exist, it will be created.
     * @param id           id for a tracker. If it does not exist, it will be created.
     * @param nanoAbsolute the amount of nanoseconds to add.
     */
    public static synchronized void time(String parent, String id, long nanoAbsolute) {
        Stats stats = trackers.get(id);
        if (stats == null) {
            stats = new Stats(id);
            trackers.put(id, stats);
        }
        stats.time(nanoAbsolute);

        // Check parent connection
        if (parent == null) {
            return;
        }

        Stats pStats = trackers.get(parent);
        if (pStats == null) {
            pStats = new Stats(parent);
            trackers.put(parent, pStats);
        }
        pStats.addChild(stats);
    }

    public static class Stats {
        public final String id;
        public final AtomicLong time = new AtomicLong(0);
        public final AtomicLong count = new AtomicLong(0);
        public final Set<Stats> children = new HashSet<Stats>();
        private static final double MD = 1000000d;

        public Stats(String id) {
            this.id = id;
        }

        public void time(long nanoAbsolute) {
            time.addAndGet(nanoAbsolute);
            count.incrementAndGet();
        }

        public String toString() {
            // % is only correct for single-threaded processing
            return String.format("%s(#=%d, time=%.2fms, avg=%.2f#/ms %.2fms/#, %.2f%%)",
                                 id, count.get(), time.get()/MD,
                                 time.get() == 0 ? 0 : count.get() / (time.get() / MD),
                                 count.get() == 0 ? 0 : time.get() / MD / count.get(),
                                 time.get() * 100.0 / (System.nanoTime()-classStart));
        }

        public void addChild(Stats child) {
            children.add(child);
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
        Set<Stats> topLevel = new HashSet<Stats>(trackers.values());
        for (Stats stats: trackers.values()) {
            for (Stats child: stats.children) {
                topLevel.remove(child);
            }
        }

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
        for (Stats child: stats.children) {
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

}
