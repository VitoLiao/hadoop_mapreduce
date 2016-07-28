package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OnlinelogMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text keyText = new Text();
	private IntWritable valueInt = new IntWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (value.getLength() > 0) {
			String []str = value.toString().split("\\|");
			
			String calcDay = str[0];
			String areaCode = str[2];
			String hdFlag = str[3];
			String type = context.getConfiguration().get("DateType");
			
			keyText.set(calcDay + "|" + areaCode + "|" + hdFlag + "|" + type);
			
			context.write(keyText, valueInt);
		}
	}
}
