package uk.bl.wa.hadoop.mapreduce;

/*
 * #%L
 * warc-hadoop-recordreaders
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

import java.io.IOException;
import java.util.Vector;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This reservoir-sampling reducer selects a finite random subset of any
 * stream of values passed to it.
 * 
 * Defaults to 1000 items in the sample.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ReservoirSamplingReducer extends Reducer<Text, Text, Text, Text> {

    private int numSamples = 1000;
    private long defaultSeed = 1231241245l;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.
     * Reducer.Context)
     */
    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        super.setup(context);
        this.numSamples = context.getConfiguration().getInt("rsr.sample.size",
                1000);
        this.defaultSeed = context.getConfiguration().getLong("rsr.seed",
                1231241245l);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object,
     * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values,
            Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {

        long numItemsSeen = 0;
        Vector<Text> reservoir = new Vector<Text>();
        RandomDataGenerator random = new RandomDataGenerator();
        // Fix the seed so reproducible by default:
        random.reSeed(defaultSeed);

        // Iterate through all values:
        for (Text item : values) {
            // Fill the reservoir:
            if (reservoir.size() < numSamples) {
                // reservoir not yet full, just append
                reservoir.add(item);
            } else {
                // find a sample to replace
                long rIndex = random.nextLong(0, numItemsSeen);
                if (rIndex < numSamples) {
                    reservoir.set((int) rIndex, item);
                }
            }
            numItemsSeen++;
        }

        // Now output the sample:
        for (Text sto : reservoir) {
            context.write(key, sto);
        }
    }

}
