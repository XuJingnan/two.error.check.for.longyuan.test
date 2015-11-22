package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by xujingnan on 11/21/15.
 */
public class ErrorCheckReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    private static final Log log = LogFactory.getLog(ErrorCheckReducer.class);

    private final IntWritable one = new IntWritable(1);
    private final IntWritable two = new IntWritable(2);

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
                break;
            }
        }
    }
}
