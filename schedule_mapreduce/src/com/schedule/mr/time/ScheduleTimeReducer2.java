package com.schedule.mr.time;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* @author VitoLiao
* @version 2016年3月1日 下午3:24:49
*/
public class ScheduleTimeReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable valueInt = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		int usersCount = 0;
		
		for (IntWritable value : values) {
			usersCount += value.get();
		}
		valueInt.set(usersCount);
		
		context.write(key, valueInt);
	}
}
