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
import java.util.StringTokenizer;

class LeastSquareReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Double a = 0.0;
        Double b = 0.0;

        List<Double> keys = new ArrayList<Double>();
        List<Double> values2 = new ArrayList<Double>();

        for(Text v : values){
            String test = String.valueOf(v);
            //let's try the tokenizer
//            double x = Double.parseDouble(test.split("-")[0]);
//            double y = Double.parseDouble(test.split("-")[1]);
            StringTokenizer st = new StringTokenizer(test, "-");
            Double x = Double.parseDouble(st.nextElement().toString());
            Double y = Double.parseDouble(st.nextElement().toString());
            if(!Double.isNaN(x)) keys.add(x);
            if(!Double.isNaN(y)) values2.add(y);
        }

        //average of x
        Double sum = 0.0;
        Double count = 0.0;
        for (double x: keys) {
            sum = sum + x;
            count = count +1;
        }
        Double xAvg = sum/count;

        //average of y
        Double sum2 = 0.0;
        Double count2 = 0.0;
        for (double y: values2) {
            sum2 = sum2 + y;
            count2++;
        }
        Double yAvg = sum2/count2;

        int size = keys.size();
        Double numerador = 0.0;
        Double denominador = 0.0;
        //xi and yi
        for(int i=0;i<size;i++){
            double xi = keys.get(i);
            double yi = values2.get(i);
            numerador = numerador +(xi * (yi - yAvg));
            denominador = denominador +  (xi * (xi - xAvg));
        }
        b = numerador/denominador;
        a = yAvg - (b*xAvg);
        Double Y1 = a + b*keys.get(0);
        Double Y2 = a + b*keys.get(keys.size() - 1);
        context.write(key, new Text(String.valueOf(a)+"-"+String.valueOf(b)+"-"+String.valueOf(Y1)+"-"+String.valueOf(Y2)));
    }
}