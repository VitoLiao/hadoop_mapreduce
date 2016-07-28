package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LostUserCombin1 extends Reducer<Text, Text, Text, Text>{
	
	Text valuetext = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int timeSum = 0;
		String strtimeSum=null;
		
		for (Text value : values) {
			int usetime=Integer.parseInt(value.toString());
			timeSum += usetime;
		}
		strtimeSum=String.valueOf(timeSum);
		valuetext.set(strtimeSum);
		context.write(key, valuetext);
			}
}
