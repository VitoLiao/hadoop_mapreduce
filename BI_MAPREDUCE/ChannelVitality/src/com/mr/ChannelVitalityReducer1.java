package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelVitalityReducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		//ͳ���û��������
		int usersClickTimes = 0;
		
		//ͳ���û��������
		for (IntWritable value : values) {
			usersClickTimes += value.get();
		}
	
		context.write(key, new IntWritable(usersClickTimes));
		
	}
}
