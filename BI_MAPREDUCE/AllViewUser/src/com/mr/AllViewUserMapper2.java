package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AllViewUserMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|HOUR|UserId 1
		// output
		// DATE|AreaCode|HdFlag|HOUR	UserId
		//or DATE|AreaCode|HdFlag	UserId
		String[] str = value.toString().trim().split("\\|");
		keyText.set(str[0]+"|"+str[1]+"|"+str[2]+"|"+str[3]);
		valueText.set(str[5]);
		context.write(keyText, valueText);
		
		keyText.set(str[0]+"|"+str[1]+"|"+str[2]);
		valueText.set("1");
		context.write(keyText, valueText);
	}
}
