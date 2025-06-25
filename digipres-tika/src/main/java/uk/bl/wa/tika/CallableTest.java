/**
 * 
 */
package uk.bl.wa.tika;

/*
 * #%L
 * digipres-tika
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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Demonstrates timeout-safe task execution with cooperative interruption.
 * 
 * @author Andrew Jackson
 */
public class CallableTest {
    public static void main(String[] args) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<String> future = executor.submit(new Task());

        try {
            System.out.println("Started..");
            System.out.println(future.get(3, TimeUnit.SECONDS)); // Wait up to 3s
            System.out.println("Finished!");
        } catch (TimeoutException e) {
            System.out.println("Terminated due to timeout!");
            future.cancel(true); // Cooperative cancel on timeout
        } finally {
            executor.shutdownNow();
        }
    }
}

class Task implements Callable<String> {
    @Override
    public String call() throws Exception {
        while (!Thread.currentThread().isInterrupted()) {
            Thread.sleep(100); // Simulate ongoing work
        }
        return "Cancelled!";
    }
}
