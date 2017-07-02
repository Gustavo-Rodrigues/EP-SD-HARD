package com.semantix;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

class StandardDeviationReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable result = new DoubleWritable();

    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double max_temp = 0;
        double count = 0;
        double average = 0;
        for (DoubleWritable value : values) {
            max_temp += value.get();
            count+=1;
        }
        average = max_temp/count;

        double summation = 0;
        for (DoubleWritable value : values) {
            double square = value.get() - average;
            square *= square;
            summation += square;
        }
        result.set(Math.sqrt(summation/(count -1)));
        context.write(key, result);
    }
}