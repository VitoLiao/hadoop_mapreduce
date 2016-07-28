package com.mr;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AllViewUserReducer1 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|HOUR|UserId 1
		// output
		// DATE|AreaCode|HdFlag|HOUR|UserId 1
		keyText.set(key);
		valueText.set("1");
		context.write(keyText, valueText);
	}
}
