package com.semantix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class LeastSquareReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double a = 0.0;
        Double b = 0.0;

        List<Double> keys = new ArrayList<Double>();
        List<Double> values2 = new ArrayList<Double>();

        for(Text v : values){
            double data[] = new double[2];
            data[0] = Double.parseDouble(v.toString().split(" ")[0]);
            data[1] = Double.parseDouble(v.toString().split(" ")[1]);
            keys.add(data[0]);
            values2.add(data[1]);
        }

        //average of x
        double sum = 0;
        double count = 0.0;
        for (double x: keys) {
            sum = sum + x;
            count++;
        }
        double xAvg = sum/count;

        //average of y
        sum = 0;
        count = 0.0;
        for (double x: values2) {
            sum = sum + x;
            count++;
        }
        double yAvg = sum/count;

        int size = keys.size();
        //xi and yi
        for(int i=0;i<size;i++){
            double xi = keys.get(i);
            double yi = values2.get(i);
            double numerator = xi * (yi - yAvg);
            double denominator = xi * (xi - xAvg);
            b = b + (numerator/denominator);
        }
        a = yAvg - (b*xAvg);
        context.write(new Text(a.toString()), new Text(b.toString()));
    }
}