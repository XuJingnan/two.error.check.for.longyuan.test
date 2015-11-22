package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by xujingnan on 11/21/15.
 */
public class ErrorCheckMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Log log = LogFactory.getLog(ErrorCheckMapper.class);

    private final IntWritable one = new IntWritable(1);
    private final IntWritable two = new IntWritable(2);
    // save output keys which satisfy check condition 1 or check condition 2
    private HashSet<Text> set1, set2;

    private Check check1, check2;
    private String separator;
    private String[] record;

    private final int timeIndex = 0, idIndex = 1, maxSetSize = 10000;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration config = context.getConfiguration();
        check1 = new Check(config, true);
        check2 = new Check(config, false);
        separator = Tools.getSeparator(config);
        set1 = new HashSet<Text>();
        set2 = new HashSet<Text>();
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
        record = value.toString().split(separator);
        if (check1.match()) {
            if (set1.size() == maxSetSize) {
                write(context, set1, one);
            }
            set1.add(new Text(record[idIndex]));
        }
        if (check2.match()) {
            if (set2.size() == maxSetSize) {
                write(context, set2, two);
            }
            set2.add(new Text(record[idIndex]));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        write(context, set1, one);
        write(context, set2, two);
        super.cleanup(context);
    }

    private void write(Context context, HashSet<Text> set, IntWritable value) throws IOException, InterruptedException {
        for (Text key : set) {
            context.write(key, value);
        }
        set.clear();
    }

    class Check {
        private String startTime;
        private String endTime;
        private int tagIndex;
        private double tagThreshold;

        public Check(Configuration config, boolean isFirst) {
            if (isFirst) {
                startTime = Tools.getString(config, Tools.firstStartTime);
                endTime = Tools.getString(config, Tools.firstEndTime);
                tagIndex = Tools.getInt(config, Tools.firstTagIndex, 1) + Tools.preIndex - 1;
                tagThreshold = Tools.getDouble(config, Tools.firstTagThreshold, 0.0);
            } else {
                startTime = Tools.getString(config, Tools.secondStartTime);
                endTime = Tools.getString(config, Tools.secondEndTime);
                tagIndex = Tools.getInt(config, Tools.secondTagIndex, 1) + Tools.preIndex - 1;
                tagThreshold = Tools.getDouble(config, Tools.secondTagThreshold, 0.0);
            }
            log.info(this);
        }

        public boolean match() {
            return between() && overThreshold();
        }

        private boolean between() {
            String checkTime = record[timeIndex];
            return (startTime.compareTo(checkTime) < 0) && (endTime.compareTo(checkTime) > 0);
        }

        private boolean overThreshold() {
            return Double.parseDouble(record[tagIndex]) > tagThreshold;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("[start time:" + startTime + ",");
            builder.append("end time:" + endTime + ",");
            builder.append("tag index:" + tagIndex + ",");
            builder.append("tag threshold:" + tagThreshold + "]");
            return builder.toString();
        }
    }
}
