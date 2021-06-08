package uk.bl.wa.hadoop.mapred;

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
import java.util.Iterator;
import java.util.Vector;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

/**
 * This reservoir-sampling reducer selects a finite random subset of any
 * stream of values passed to it.
 * 
 * Defaults to 1000 items in the sample.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ReservoirSamplingReducer extends MapReduceBase
        implements Reducer<Text, Text, Text, Text> {

    private int numSamples = 1000;
    private long defaultSeed = 1231241245l;
    private MultipleOutputs mos;

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.hadoop.mapred.MapReduceBase#configure(org.apache.hadoop
     * .mapred .JobConf)
     */
    @Override
    public void configure(JobConf job) {
        super.configure(job);
        mos = new MultipleOutputs(job);
    }

    @Override
    public void reduce(Text key, Iterator<Text> values,
            OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        Text item;
        long numItemsSeen = 0;
        Vector<Text> reservoir = new Vector<Text>();
        RandomDataGenerator random = new RandomDataGenerator();
        // Fix the seed so repoducible by default:
        random.reSeed(defaultSeed);

        // Iterate through all values:
        while (values.hasNext()) {
            item = values.next();

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

        // Choose the output:
        Text outKey = key;
        OutputCollector<Text, Text> collector;
        int pos = key.find("__");
        if (pos == -1) {
            collector = output;
        } else {
            String[] fp = key.toString().split("__");
            collector = getCollector(fp[0], fp[1], reporter);
            outKey = new Text(fp[1]);
        }

        // Now output the sample:
        for (Text sto : reservoir) {
            collector.collect(outKey, sto);
        }
    }

    @SuppressWarnings("unchecked")
    private OutputCollector<Text, Text> getCollector(String fp, String fp2,
            Reporter reporter) throws IOException {
        return mos.getCollector(fp, fp2, reporter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.mapred.MapReduceBase#close()
     */
    @Override
    public void close() throws IOException {
        super.close();
        mos.close();
    }

}
