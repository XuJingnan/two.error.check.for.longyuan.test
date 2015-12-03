package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by xujingnan on 11/21/15.
 */
public class ErrorCheckProcessor extends Configured implements Tool {
    Log log = LogFactory.getLog(ErrorCheckProcessor.class);

    /*
    arg0: input path
    arg1: output path

    arg2: first start time
    arg3: first end time
    arg4: first tag index
    arg5: first tag threshold

    arg6: second start time
    arg7: second end time
    arg8: second tag index
    arg9: second tag threshold

    arg10: separator flag [:0":","    "1":"\001"    other string:"\t"]

    arg11: reduce number
    arg12: split size(MB):256 ~ 1024

    cmd example:
    hadoop jar longyuan.test.error.check-1.0.jar com.envision.ErrorCheckProcessor input/machinedata output/mr/check "2015-11-11 00:00:00" "2015-11-11 00:00:05" 1 40.0 "2015-11-11 00:00:11" "2015-11-11 00:00:15" 1 30.0 0 2 1024
     */
    public int run(String[] args) throws Exception {
        log.info("starting");
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        conf = job.getConfiguration();

        Path inputDir = new Path(args[0]);
        Path outDir = new Path(args[1]);
        FileSystem fs = outDir.getFileSystem(conf);
        fs.delete(outDir, true);

        Tools.setString(conf, Tools.firstStartTime, args[2]);
        Tools.setString(conf, Tools.firstEndTime, args[3]);
        Tools.setInt(conf, Tools.firstTagIndex, Integer.parseInt(args[4]));
        Tools.setDouble(conf, Tools.firstTagThreshold, Double.parseDouble(args[5]));

        Tools.setString(conf, Tools.secondStartTime, args[6]);
        Tools.setString(conf, Tools.secondEndTime, args[7]);
        Tools.setInt(conf, Tools.secondTagIndex, Integer.parseInt(args[8]));
        Tools.setDouble(conf, Tools.secondTagThreshold, Double.parseDouble(args[9]));

        Tools.setString(conf, Tools.separatorFlag, args[10]);

        int reduceNumber = Integer.parseInt(args[11]);
        job.setNumReduceTasks(reduceNumber);

        job.setJobName("ErrorCheck");
        job.setJarByClass(ErrorCheckProcessor.class);
        job.setMapperClass(ErrorCheckMapper.class);
        job.setReducerClass(ErrorCheckReducer.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        job.getConfiguration().setLong(FileInputFormat.SPLIT_MAXSIZE, Integer.parseInt(args[12]) * 1024 * 1024);
        job.getConfiguration().setLong(FileInputFormat.SPLIT_MINSIZE, Integer.parseInt(args[12]) * 1024 * 1024);
        FileInputFormat.setInputDirRecursive(job, true);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outDir);

        job.waitForCompletion(true);
        log.info("done");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(null, new ErrorCheckProcessor(), args);
        System.exit(res);
    }
}
