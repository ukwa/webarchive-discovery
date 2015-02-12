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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Helper class for lightweight performance-measuring instrumentation of the code base.
 * By nature the class is shared by the whole JVM.
 * </p><p>
 * Usage: Call {@code Instrument.time(customid, nanotime)} at relevant places in the code.
 */
public class Instrument {
    private static Log log = LogFactory.getLog(Instrument.class);

    private static final Map<String, Stats> trackers = new HashMap<String, Stats>();

    /**
     * Increment the total time for the tracker with the given ID.
     * The delta is calculated with {@code System.nanotime() - nanoStart}.
     * @param id        id for a tracker. If it does not exist, it will be created.
     * @param nanoStart the start time of the measurement.
     */
    public static void timeRel(String id, long nanoStart) {
        time(id, System.nanoTime() - nanoStart);
    }

    /**
     * Increment the total time for the tracker with the given ID.
     * Synchronized to handle creation of new Stats.
     * @param id           id for a tracker. If it does not exist, it will be created.
     * @param nanoAbsolute the amount of nanoseconds to add.
     */
    public static synchronized void time(String id, long nanoAbsolute) {
        Stats stats = trackers.get(id);
        if (stats == null) {
            stats = new Stats(id);
            trackers.put(id, stats);
        }
        stats.time(nanoAbsolute);
    }

    public static class Stats {
        public final String id;
        public final AtomicLong time = new AtomicLong(0);
        public final AtomicLong count = new AtomicLong(0);
        private static final double MD = 1000000d;

        public Stats(String id) {
            this.id = id;
        }

        public void time(long nanoAbsolute) {
            time.addAndGet(nanoAbsolute);
            count.incrementAndGet();
        }

        public String toString() {
            return String.format("%s(num=%d, time=%.2fms, avg=%.2fnum/ms %.2fms/num",
                                 id, count.get(), time.get()/MD,
                                 time.get() == 0 ? 0 : count.get() / (time.get() / MD),
                                 count.get() == 0 ? 0 : time.get() / MD / time.get());
        }
    }

    /**
     * Logs collected statistics.
     * @param major if true, the report is logged at INFO level. If false, DEBUG.
     */
    public static void log(boolean major) {
        if (major) {
            log.info(getStats());
        } else {
            log.debug(getStats());
        }
    }

    private static String getStats() {
        StringBuilder sb = new StringBuilder();
        for (Stats stats: trackers.values()) {
            if (sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(stats);
        }
        return sb.toString();
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
