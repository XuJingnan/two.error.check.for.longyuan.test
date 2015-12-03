package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by xujingnan on 11/21/15.
 */
public class ErrorCheckReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    private static final Log log = LogFactory.getLog(ErrorCheckReducer.class);

    private final IntWritable one = new IntWritable(1);
    private final IntWritable two = new IntWritable(2);
    private BufferedWriter out;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Path intervalOutputDir = new Path(conf.get(FileOutputFormat.OUTDIR)
                + ".log/"
                + Tools.getID(context.getTaskAttemptID().toString(), false));
        out = new BufferedWriter(new OutputStreamWriter(intervalOutputDir.getFileSystem(conf).create(intervalOutputDir)));
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        boolean match1 = false, match2 = false;
        for (IntWritable value : values) {
            if (value.equals(one)) {
                match1 = true;
            } else if (value.equals(two)) {
                match2 = true;
            }
            if (match1 && match2) {
                context.write(key, NullWritable.get());
                out.write(key.toString() + "\n");
                break;
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        out.close();
        super.cleanup(context);
    }
}
