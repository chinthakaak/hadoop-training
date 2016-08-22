package com.psl.accfreq;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AccessFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {
	private Text word = new Text();
	private final static IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// Key: 0
		// Value: One two three
        String[] splited = value.toString().split(" ");        
        
		word.set(splited[2]+" hit");
		context.write(word, one);
	}
}
