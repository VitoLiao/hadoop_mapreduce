package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
* @author VitoLiao
* @version 2016��3��1�� ����3:24:49
*/
public class ScheduleTimeReducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable valueInt = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		int timeSum = 0;
		
		for (IntWritable value : values) {
			timeSum += value.get();
		}
		
		// ʱ���־λ�� [1] : x<10min, [2] : 10min <= x < 1h, [3] : 1h <= x < 2h,
		// [4] : 2h <= x < 3h, [5] : 3h <= x < 4h, [6] : 4h <= x
		int timeFlag = 0;
		if (timeSum < 10 * 60) {
			timeFlag = 1;
		} else if (timeSum >= 10 * 60 && timeSum < 60 * 60) {
			timeFlag = 2;
		} else if (timeSum >= 60 * 60 && timeSum < 120 * 60) {
			timeFlag = 3;
		} else if (timeSum >= 120 * 60 && timeSum < 180 * 60) {
			timeFlag = 4;
		} else if (timeSum >= 180 * 60 && timeSum < 240 * 60) {
			timeFlag = 5;
		} else if (timeSum >= 240 * 60) {
			timeFlag = 6;
		}

		valueInt.set(timeFlag);
		
		context.write(key, valueInt);
	}
}
