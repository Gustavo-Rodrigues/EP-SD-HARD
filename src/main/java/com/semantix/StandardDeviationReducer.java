package com.semantix;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

class StandardDeviationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        //Avg

//        https://stackoverflow.com/questions/15530906/why-doesnt-this-code-iterate-through-the-reducer-values-twice
        //test for for possible iterator problem
        List<Double> test = new ArrayList<Double>();

        //average code
        double sum = 0;
        double count = 0.0;
        for (DoubleWritable value : values) {
            sum = sum + value.get();
            test.add(value.get());
            count++;
        }
        double avg = (sum/count);

        Double summation = 0.0;
        for (Double data : test) {
            summation = summation + Math.pow((data - avg), 2);
        }

        Double result = Math.sqrt(summation/(count-1));
        context.write(key, new DoubleWritable(result));
    }
}