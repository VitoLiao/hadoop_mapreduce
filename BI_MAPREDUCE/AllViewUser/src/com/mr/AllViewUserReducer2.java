package com.mr;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AllViewUserReducer2 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|HOUR	UserId
		// output
		// DATE|AreaCode|HdFlag|HOUR	viewTime
		// or DATE|AreaCode|HdFlag 	viewTimeAll
		long viewTime = 0;
		long viewTimeAll = 0;
		String[] str = key.toString().split("\\|",-1);
		
		for(Text value : values){
			if (str.length==3) {
				viewTimeAll += 1;
			}else {
				viewTime += 1;
			}
		}
		
		if (str.length==3) {
			BigInteger bivta = BigInteger.valueOf(viewTimeAll);
			keyText.set(key);
			valueText.set(String.valueOf(bivta));
			context.write(keyText, valueText);
		}else {
			BigInteger bivt = BigInteger.valueOf(viewTime);
			keyText.set(key);
			valueText.set(String.valueOf(bivt));
			context.write(keyText, valueText);
		}
		
	}
}
