package com.semantix;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main{

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("start", args[2]);
        conf.set("end", args[3]);
        conf.set("data", args[4]); //rain, snow, etc..
        conf.set("granularity", args[5]);

        String method = args[6];
        Job job = Job.getInstance(conf, "ep");
        job.setJarByClass(Main.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //media
        //desvio padrão
        //variância
        //maximo
        //minimo
        //minimos quadrados
        switch(method) {
            case "avg":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setMapperClass(AverageMapper.class);
                job.setReducerClass(AverageReducer.class);
                break;
            case "sd":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setMapperClass(AverageMapper.class);
                job.setReducerClass(StandardDeviationReducer.class);
                break;
            case "var":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setMapperClass(AverageMapper.class);
                job.setReducerClass(VarianceReducer.class);
                break;
            case "max":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setMapperClass(AverageMapper.class);
                job.setReducerClass(MaxReducer.class);
                break;
            case "min":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setMapperClass(AverageMapper.class);
                job.setReducerClass(MinReducer.class);
                break;
            case "square":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                job.setMapperClass(LeastSquareMapper.class);
                job.setReducerClass(LeastSquareReducer.class);
                break;
            case "square2":
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(DoubleWritable.class);
                job.setMapperClass(MinimumSquareMapper.class);
                job.setReducerClass(MinimumSquareReducer.class);
                break;
        }
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}