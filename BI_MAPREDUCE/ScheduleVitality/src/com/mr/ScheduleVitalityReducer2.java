package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* @author VitoLiao
* @version 2016��3��1�� ����3:24:49
*/
public class ScheduleVitalityReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable valueInt = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable value : values) {
			sum += value.get();
		}

		valueInt.set(sum);
		
		context.write(key, valueInt);
	}
}
