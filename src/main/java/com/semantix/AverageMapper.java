package com.semantix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Created by semantix on 01/07/17.
 */
public class AverageMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String data = conf.get("data");
        String start = conf.get("start");
        String end = conf.get("end");
        String granularity = conf.get("granularity");

        Translate tr = new Translate();

        String line = value.toString();

        //check if row is inside the period
        int currentYear = Integer.parseInt(line.substring(14,18));
        if (currentYear < Integer.parseInt(start) || currentYear > Integer.parseInt(end))
            return;

        //year, month, day, position
        int[] time = new int[2];
        time = tr.getData(granularity);

        int[] data_pos = new int[2];
        data_pos = tr.getData(data);

        double content = 0.0;
        String group = line.substring(time[0], time[1]);
        content = Double.parseDouble(line.substring(data_pos[0],data_pos[1]));

        if(content != 999.9 && content != 99.99) context.write(new Text(group),new DoubleWritable(content));

    }
}