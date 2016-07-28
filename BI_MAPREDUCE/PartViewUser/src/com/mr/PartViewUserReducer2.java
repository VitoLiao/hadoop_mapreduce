package com.mr;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartViewUserReducer2 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|LogType|HOUR	UserId
		// DATE|AreaCode|HdFlag|LogType	UserId
		// output
		// DATE|AreaCode|HdFlag|LogType|HOUR	viewTime
		// DATE|AreaCode|HdFlag|LogType	viewTimeAll
		long viewTime = 0;
		long viewTimeAll = 0;
		String[] str = key.toString().split("\\|",-1);
		int len = str.length;
		for(Text value : values){
			if(len==4){
				viewTimeAll += 1;
			}else if(len==5){
				viewTime += 1;
			}
		}
		if(len==4){
			BigInteger bivta = BigInteger.valueOf(viewTimeAll);
			valueText.set(String.valueOf(bivta));
		}else if(len == 5) {
			BigInteger bivt = BigInteger.valueOf(viewTime);
			valueText.set(String.valueOf(bivt));
		}
		keyText.set(key);
		context.write(keyText, valueText);
	}
}
