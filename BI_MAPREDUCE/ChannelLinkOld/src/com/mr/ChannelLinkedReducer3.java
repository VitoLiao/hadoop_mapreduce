package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ChannelLinkedReducer3 extends Reducer<Text, Text, Text, Text> {
//	private Text valueInt = new Text();
	private Text keyText = new Text();
	private Text valueText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int countUser = 0;
		int countIntervalTime = 0;
		for (Text value : values){
			String []strValue = value.toString().trim().split("\\|");
			countUser += 1;
			countIntervalTime += Integer.valueOf(strValue[1]);
		}
		
		keyText.set(key);
		valueText.set(countUser + "|" + countIntervalTime + "|1");
		context.write(keyText, valueText);
//		System.out.println("output= " +keyText + "|" + valueText);
	}

}
