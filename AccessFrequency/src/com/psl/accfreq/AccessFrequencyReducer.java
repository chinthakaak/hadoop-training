package com.psl.accfreq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AccessFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	// e.g. - 
	// One,list<1, 1>
	// Two,<1>
	// Three, <1,1,1>
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		IntWritable result = new IntWritable(0);
		
		for(IntWritable val: value) {
				sum+=val.get();
		}
		// One, 2
		result.set(sum);
		context.write(key, result);
	}
}
