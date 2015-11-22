package com.envision;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by xujingnan on 11/21/15.
 */
public class Tools {
    public static final String firstStartTime = "error.check.first.start.time";
    public static final String firstEndTime = "error.check.first.end.time";
    public static final String secondStartTime = "error.check.second.start.time";
    public static final String secondEndTime = "error.check.second.end.time";

    public static final String separatorFlag = "error.check.map.input.record.separator.flag";

    public static final String firstTagIndex = "error.check.first.tag.index";
    public static final String firstTagThreshold = "error.check.first.tag.threshold";
    public static final String secondTagIndex = "error.check.second.tag.index";
    public static final String secondTagThreshold = "error.check.second.tag.threshold";

    public static final int preIndex = 352;

    public static void setString(Configuration config, String name, String value) {
        config.set(name, value);
    }

    public static String getString(Configuration config, String name) {
        return config.get(name);
    }

    public static String getString(Configuration config, String name, String defaultValue) {
        return config.get(name, defaultValue);
    }

    public static String getSeparator(Configuration config) {
        String flag = getString(config, separatorFlag, "0");
        return flag.equals("0") ? "," : (flag.equals("1") ? "\001" : "\t");
    }

    public static int getInt(Configuration config, String name, int defaultValue) {
        return config.getInt(name, defaultValue);
    }

    public static void setInt(Configuration config, String name, int value) {
        config.setInt(name, value);
    }

    public static double getDouble(Configuration config, String name, double defaultValue) {
        return config.getDouble(name, defaultValue);
    }

    public static void setDouble(Configuration config, String name, double value) {
        config.setDouble(name, value);
    }
}
