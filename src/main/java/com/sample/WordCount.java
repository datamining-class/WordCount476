package com.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
/*
IntWritable is the Hadoop variant of Integer which has been optimized for serialization in the Hadoop environment.
An integer would use the default Java Serialization which is very costly in Hadoop environment.
see: https://stackoverflow.com/questions/52361265/integer-and-intwritable-types-existence
 */

public class WordCount {

    // We have created a class TokenizerMapper that extends
    // the class Mapper which is already defined in the MapReduce Framework.
    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, Text, IntWritable>{

        // Output:
        // We have the hardcoded value in our case which is 1: IntWritable
        private final static IntWritable one = new IntWritable(1);
        // The key is the tokenized words: Text
        private Text word = new Text();

        // Input:
        // We define the data types of input and output key/value pair after the class declaration using angle brackets.
        // Both the input and output of the Mapper is a key/value pair.

        // The key is nothing but the offset of each line in the text file: LongWritable
        // The value is each individual line (as shown in the figure at the right): Text
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
        // We have written a java code where we have tokenized each word
        // and assigned them a hardcoded value equal to 1.
        // Eg: Dear 1, Bear 1,
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        // Input:
        // The key nothing but those unique words which have been generated after the sorting and shuffling phase: Text
        // The value is a list of integers corresponding to each key: IntWritable
        // Eg: Bear, [1, 1],
        // Output:
        // The key is all the unique words present in the input text file: Text
        // The value is the number of occurrences of each of the unique words: IntWritable
        // Eg: Bear, 2; Car, 3,

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static String outputPath;
    private static String inputPath;

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--input_path")) {
                inputPath = args[++i];
            } else if (args[i].equals("--output_path")) {
                outputPath = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        if (outputPath == null || inputPath == null) {
            throw new RuntimeException("Either outputpath or input path are not defined");
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs476");         // required for this to work on GreatLakes


        Job wordCountJob = Job.getInstance(conf, "wordCountJob");
        wordCountJob.setJarByClass(WordCount.class);
        wordCountJob.setNumReduceTasks(1);

        wordCountJob.setMapperClass(TokenizerMapper.class);
        wordCountJob.setReducerClass(IntSumReducer.class);

        // set mapper output key and value class
        // if mapper and reducer output are the same types, you skip
        wordCountJob.setMapOutputKeyClass(Text.class);
        wordCountJob.setMapOutputValueClass(IntWritable.class);

        // set reducer output key and value class
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wordCountJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(outputPath));

        wordCountJob.waitForCompletion(true);
    }

}
