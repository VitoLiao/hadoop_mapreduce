package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserViewAreaMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		// input
		// key value
		// a|DATE|AreaCode|LogType|viewTime|viewInterval  or  b|DATE|AreaCode|UserId|1
		// output 
		// key valut
		// DATE|AreaCode LogType|viewTime|viewInterval  or  b|DATE|AreaCode UserId
		
//		keyText.set(value);
//		valueText.set("");
//		context.write(keyText, valueText);
		
		String[] str = value.toString().trim().split("\\|",-1);
//		System.out.println("M2 value"+ value);
		if(str[0].equals("b")){
			keyText.set("b"+"|"+str[1]+"|"+str[2]);
			valueText.set(str[3]);
			context.write(keyText, valueText);
//			System.out.println("M2 1");
		}else if(str[0].equals("a")){
			keyText.set(str[1]+"|"+str[2]);
			valueText.set(str[3]+"|"+str[4]+"|"+str[5]);
			context.write(keyText, valueText);
//			System.out.println("M2 2");
		}
	}
}
