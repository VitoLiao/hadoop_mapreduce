package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PartViewUserMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|LogType|HOUR	viewTime
		// DATE|AreaCode|HdFlag|LogType	viewTimeAll
		// output
		// DATE|AreaCode|HdFlag|LogType	HOUR|viewTime
		String[] str = value.toString().trim().split("\\|",-1);
//		System.out.println("size value= "+str.length+" "+value);
		if(str.length==5){
			keyText.set(str[0]+"|"+str[1]+"|"+str[2]+"|"+str[3]);
			valueText.set(str[4]);
			context.write(keyText, valueText);
		}else if(str.length==6){
			keyText.set(str[0]+"|"+str[1]+"|"+str[2]+"|"+str[3]);
			valueText.set(str[4]+"|"+str[5]);
			context.write(keyText, valueText);
		}
	}
}
