package com.semantix;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.ArrayList;
import java.lang.Math;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class MinimumSquareReducer extends Reducer<IntWritable, Text, DoubleWritable, DoubleWritable>{


    //quick sort that sorts one vector and changes the other in the corresponding positions
    public static <T extends Comparable<T>, S extends Comparable<S>> void quickSortModified(ArrayList <T> vetor, ArrayList<S> vetor2, int inicio, int fim) {

        if (inicio < fim) {
            int posicaoPivo = separar(vetor, vetor2, inicio, fim);
            quickSortModified(vetor, vetor2, inicio, posicaoPivo - 1);
            quickSortModified(vetor, vetor2, posicaoPivo + 1, fim);
        }
    }

    public static <T extends Comparable<T>, S extends Comparable<S>> int separar(ArrayList<T> vetor, ArrayList<S> vetor2, int inicio, int fim) {

        T pivo = vetor.get(inicio);
        S pivo2 = vetor2.get(inicio);
        int i = inicio + 1, f = fim;
        while (i <= f) {
            if (vetor.get(i).compareTo(pivo) <= 0)
                i++;
            else if (vetor.get(f).compareTo(pivo) > 0)
                f--;
            else {
                T troca = vetor.get(i);
                vetor.set(i, vetor.get(f));
                vetor.set(f, troca);
                S troca2 = vetor2.get(i);
                vetor2.set(i, vetor2.get(f));
                vetor2.set(f, troca2);
                i++;
                f--;
            }
        }
        vetor.set(inicio, vetor.get(f));
        vetor.set(f, pivo);
        vetor2.set(inicio, vetor2.get(f));
        vetor2.set(f, pivo2);
        return f;
    }

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException, NullPointerException{

        ArrayList <Date> dates = new ArrayList(); 		//Temporary X axis
        ArrayList <Double> YAxis = new ArrayList();    //Y axis

        for (Text value : values){
            String currentValue = null;
            SimpleDateFormat format = null;
            Date formatedDate = null;
            try {
                currentValue = value.toString();
                format = new SimpleDateFormat("yyyyMMdd");
                formatedDate = format.parse(currentValue.substring(0, 8));
                Double variable = Double.parseDouble(currentValue.substring(9, currentValue.length()));
                YAxis.add(variable);
                dates.add(formatedDate);
            }catch (ParseException e){
                e.printStackTrace();
            }
        }
        quickSortModified(dates, YAxis, 0 ,dates.size() - 1);

        ArrayList <Integer> XAxis = new ArrayList(dates.size()); 		//Permanent X axis
        for (Integer i = 0; i < dates.size(); i++)
            XAxis.add(i,i);

		/*Calculate mean for X */
        double sum = 0;
        for (int i = 0; i < XAxis.size(); i++) {
            sum += XAxis.get(i);
        }
        double MeanX = sum / XAxis.size();

		/*Calculate mean for Y */
        sum = 0;
        for (int i = 0; i < YAxis.size(); i++) {
            sum += YAxis.get(i);
        }
        double MeanY = sum / YAxis.size();

        Double A = 0.0;
        Double B = 0.0;
        Double somaDividendo = 0.0;
        Double somaDivisor = 0.0;

		/*Calculate B */
        for (int i = 0; i < XAxis.size(); i++){
            somaDividendo = somaDividendo + ( (XAxis.get(i) * YAxis.get(i)) - (XAxis.get(i) * MeanY) );
            somaDivisor = somaDivisor + ( (XAxis.get(i) * XAxis.get(i)) - (XAxis.get(i) * MeanX) );
            B = somaDividendo / somaDivisor;
        }

		/*Calculate A */
        A = MeanY - B*MeanX;

        Double Y1 = A + B*XAxis.get(0);
        Double Y2 = A + B*XAxis.get(XAxis.size() - 1);

        context.write(new DoubleWritable(Y1), new DoubleWritable(Y2));
    }
}


