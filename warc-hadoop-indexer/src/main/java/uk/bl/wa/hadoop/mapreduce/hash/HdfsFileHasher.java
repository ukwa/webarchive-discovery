/**
 * 
 */
package uk.bl.wa.hadoop.mapreduce.hash;

import java.io.BufferedReader;
import java.io.FileReader;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class HdfsFileHasher extends Configured implements Tool {

    private static final Log log = LogFactory.getLog(HdfsFileHasher.class);

    /* (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    @Override
    public int run(String[] args) throws Exception {

        // When implementing tool
        Configuration conf = this.getConf();

        // Create job
        Job job = new Job(conf, "Tool Job");
        job.setJarByClass(HdfsFileHasher.class);

        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(ShaSumMapper.class);
        job.setReducerClass(Reducer.class);

        // Just one output file:
        job.setNumReduceTasks(1);

        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Input
        log.info("Reading input files...");
        String line = null;
        long line_count = 0;
        BufferedReader br = new BufferedReader(new FileReader(args[0]));
        while ((line = br.readLine()) != null) {
            if (StringUtils.isEmpty(line))
                continue;
            //
            line_count++;
            Path path = new Path(line);
            FileSystem fs = path.getFileSystem(conf);
            if (fs.isFile(path)) {
                FileInputFormat.addInputPath(job, path);
            } else if (fs.isDirectory(path)) {
                for (FileStatus fstat : fs.listStatus(path)) {
                    if (!fstat.isDir()) {
                        FileInputFormat.addInputPath(job, fstat.getPath());
                    }
                }
            }
        }
        br.close();
        log.info("Read " + FileInputFormat.getInputPaths(job).length
                + " input files from " + line_count + " paths.");
        job.setInputFormatClass(UnsplittableInputFileFormat.class);

        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new HdfsFileHasher(),
                args);
        System.exit(res);
        // TODO Auto-generated method stub

    }

}
