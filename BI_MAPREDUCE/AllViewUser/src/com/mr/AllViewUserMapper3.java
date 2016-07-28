package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AllViewUserMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag |HOUR|viewTime
		// or DATE|AreaCode|HdFlag| viewTimeAll
		//DATE|AreaCode|HdFlag 	viewTimeAll
		String[] str = value.toString().trim().split("\\|",-1);
		if(str.length==4){
			keyText.set(str[0]+"|"+str[1]+"|"+str[2]);
			valueText.set(str[3]);
		}else {
			keyText.set(str[0]+"|"+str[1]+"|"+str[2]);
			valueText.set(str[3]+"|"+str[4]);
		}
		context.write(keyText, valueText);
	}
}
