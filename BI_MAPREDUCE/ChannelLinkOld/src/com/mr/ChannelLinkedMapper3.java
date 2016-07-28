package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ChannelLinkedMapper3 extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text keyText = new Text();
	private Text valueText = new Text();
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		//input
		//map3: KEY:	VALUE;
		//日期|CONTENTTYPE|TYPE|CODE|LCODE|USERID|TIMEINTERVAL
		//output
		//map2: KEY:	VALUE;
		//日期|CONTENTTYPE|TYPE|CODE|LCODE	USERID|TIMEINTERVAL
		String []strValue = value.toString().trim().split("\\|");
		keyText.set(strValue[0] + "|" 
				+strValue[1] + "|" 
				+strValue[2] + "|" 
				+strValue[3] + "|" 
				+strValue[4]);
		valueText.set(strValue[5] + "|" + strValue[6]);
		context.write(keyText, valueText);
	}

}
