package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelVBreakMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	
//	private static final IntWritable one = new IntWritable(1);
	private Text keyText = new Text();
	private Text valueText = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
//		System.out.println("key= "+key);
//		System.out.println("value= " + value);
		//ֻ�������ݲ�Ϊ�յ�����
//		if (value.getLength() > 0) {
//			String []str = value.toString().trim().split("\\|");
//			
//			String []strnum = str[1].trim().split(",");
//			keyText.set(str[0]);
//			one.set(Integer.valueOf(strnum[0]+strnum[1]));
//		System.out.println("key= " + key);	
		keyText.set(key.toString());
		valueText.set(value);
		context.write(keyText, valueText);
//		System.out.println("M2 key= "+keyText);
//		System.out.println("M2 value= "+valueText);
//		}
	}
}
