package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TvodVitalityReducer2 extends Reducer<Text, IntWritable, Text, NullWritable>{
	
	private Text keyText = new Text();
	private IntWritable valueText = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		
		int usersCount = 0;
		
		for (IntWritable value : values) {
			usersCount += value.get();
		}
		
		valueText.set(usersCount);
		
		String []str = key.toString().split("\\|");
		
		String sday = str[0];
		String cc = str[1];
		String sc = str[2];
		String ac = str[3];
		String hdflag = str[4];
		String vc = str[5];
		
		keyText.set(cc + "|" + sc + "|" + ac + "|" + vc + "|" + valueText + "|" + sday + "|" + hdflag);
		
		context.write(keyText, NullWritable.get());
	}
}
