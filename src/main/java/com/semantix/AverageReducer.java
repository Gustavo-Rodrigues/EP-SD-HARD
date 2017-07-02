package com.semantix;

/**
 * Created by semantix on 01/07/17.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class AverageReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String data = conf.get("data");
        String start = conf.get("start");
        String end = conf.get("end");
        String period = conf.get("period");

        double sum = 0;
        int cont = 0;
        for (DoubleWritable value : values) {
            cont++;
            sum += value.get();
        }
        Double avg = (double) sum / cont;
        context.write(key, new DoubleWritable(avg));
    }
}
