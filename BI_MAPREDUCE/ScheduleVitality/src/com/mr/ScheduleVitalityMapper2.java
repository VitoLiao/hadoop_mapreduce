package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
* @author VitoLiao
* @version 2016��3��1�� ����2:07:47
*/
public class ScheduleVitalityMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
	private Text keyText = new Text();
	private IntWritable valueInt = new IntWritable();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String []str = value.toString().trim().split(",");
		
		//keyΪmonthday + channelcode + areacode + hdflag + datetype + clicktimes
		keyText.set(str[0]);
		valueInt.set(Integer.parseInt(str[1]));
		context.write(keyText, valueInt);		
	}
}
