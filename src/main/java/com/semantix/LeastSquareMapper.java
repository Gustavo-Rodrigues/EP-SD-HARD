package com.semantix;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class LeastSquareMapper extends Mapper<LongWritable, Text, Text, Text> {
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
        int currentYear = Integer.parseInt(line.substring(14, 18));

        //year, month, day, position
        int[] time = new int[2];
        time = tr.getData(granularity);

        int[] data_pos = new int[2];
        data_pos = tr.getData(data);

        Double content = 0.0;
        String group = line.substring(14,22);;
        Double time_group = Double.parseDouble(group);
        content = Double.parseDouble(line.substring(data_pos[0], data_pos[1]));

        if ( currentYear >= Integer.parseInt(start) && currentYear <= Integer.parseInt(end)){
            if (content != 999.9 && content != 99.99 && content != 9999.9) {
                context.write(new Text(String.valueOf(time_group)), new Text(String.valueOf(time_group)+"-"+String.valueOf(content)));
            }
        }
    }
}