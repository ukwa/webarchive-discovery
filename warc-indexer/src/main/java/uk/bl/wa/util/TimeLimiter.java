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

import org.apache.commons.httpclient.URIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.regex.Pattern;

/**
 * Helper class for running jobs for a specific time before giving up.
 */
public class TimeLimiter {
    private static Logger log = LoggerFactory.getLogger(TimeLimiter.class );

    /**
     * Creates a Thread with runnable and starts it. Waits at most timeoutMS for it to finish before sending an
     * interrupt. If waitAfterInterrupt is true, it then waits at most timeoutMS for the thread to finish.
     *
     * Important: Due to the nature of Java Threads the runnable might still be executing in the background after
     * this method has returned. There is not hard shutdown of the runnable.
     * @param runnable the job to run in a limited amount of time.
     * @param timeoutMS the amount of time to wait for processing to finish.
     * @param waitAfterInterrupt if true, there will be a new timeout after interrupt has been called.
     * @return true if the runnable finished within the timeout, else false.
     */
    public static boolean run(Runnable runnable, long timeoutMS, boolean waitAfterInterrupt) {
        Thread parseThread = new Thread(runnable, "timelimiter_" + Long.toString(System.currentTimeMillis()));
        parseThread.setDaemon(true); // Ensure that the JVM will not hang on exit, waiting for Threads to finish
        final long startTime = System.currentTimeMillis();
        log.debug("Starting timelimited run of " + runnable.getClass() + " with timeout " + timeoutMS + "ms");
        parseThread.start();
        try {
            parseThread.join(timeoutMS);
        } catch (InterruptedException e) {
            throw new RuntimeException("The Thread for the Runnable " + runnable.getClass() +
                                       " was interrupted while waiting for result", e);
        }

        long spendTime = System.currentTimeMillis()-startTime;
        if (spendTime <= timeoutMS) {
            // Finished within the timeout
            log.debug("Finished timelimited run of " + runnable.getClass() + " with timeout " + timeoutMS +
                      "ms successfully in " + spendTime + "ms");
            return true;
        }

        // Did not finish. Try interrupting
        parseThread.interrupt();
        if (waitAfterInterrupt) {
            try {
                parseThread.join(timeoutMS);
            } catch (InterruptedException e) {
                throw new RuntimeException(
                        "The Thread for the Runnable was interrupted while waiting for result after interrupting", e);
            }
        }
        log.debug("Finished timelimited run of " + runnable.getClass() + " with timeout " + timeoutMS +
                  "ms unsuccessfully in " + spendTime + "ms. The created Thread is still alive");
        return false;
    }
}
