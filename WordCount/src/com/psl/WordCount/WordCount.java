package com.psl.WordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		String[] otherArg = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		Job job = new Job(conf, "Word Count");
		job.setJarByClass(WordCount.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArg[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArg[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
