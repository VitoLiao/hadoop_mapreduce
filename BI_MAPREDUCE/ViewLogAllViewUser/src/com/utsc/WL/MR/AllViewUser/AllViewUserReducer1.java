package com.utsc.WL.MR.AllViewUser;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AllViewUserReducer1 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|HOUR|UserId 1
		// or DATE|AreaCode|HdFlag|24|UserId 1
		// output
		// DATE|AreaCode|HdFlag|HOUR|UserId 1
		for(Text value : values){
			valueText.set(value);
			keyText.set(key);
			
		}
		context.write(keyText, valueText);
	}
}
