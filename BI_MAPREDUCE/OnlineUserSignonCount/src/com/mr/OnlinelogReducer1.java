package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnlinelogReducer1 extends Reducer<Text, IntWritable, Text, LongWritable> {

	private LongWritable valueText = new LongWritable(1);
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		long countSum = 0;

		for (IntWritable value : values) {
			countSum += value.get();
		}

		valueText.set(countSum);
		context.write(key, valueText);
	}
}
