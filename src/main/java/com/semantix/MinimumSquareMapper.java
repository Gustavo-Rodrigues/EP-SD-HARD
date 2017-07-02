package com.semantix;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class MinimumSquareMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String data = conf.get("data");
        String start = conf.get("start");
        String end = conf.get("end");

        Translate tr = new Translate();
        String line = value.toString();
        int currentYear = Integer.parseInt(line.substring(14,18));

        int[] data_pos = new int[2];
        data_pos = tr.getData(data);

        String content = null;

        content = content + line.substring(14,22);
        content = content + " ";
        Double data2 = Double.parseDouble(line.substring(data_pos[0],data_pos[1]));
        content = content + String.valueOf(data);

        if (currentYear < Integer.parseInt(start) && currentYear > Integer.parseInt(end)){
            if(data2 != 999.9 && data2 != 99.99 && data2 != 9999.9) context.write(new IntWritable(1),new Text(content));
        }

    }
}