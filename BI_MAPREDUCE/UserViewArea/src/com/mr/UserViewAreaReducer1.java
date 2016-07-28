package com.mr;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserViewAreaReducer1 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// output
		// KEY: VALUE:
		// a|DATE|AreaCode|LogType	TimeInterval   or  b|DATE|AreaCode|UserId	1
		// output
		// a|DATE|AreaCode|LogType|viewTime|viewInterval   or  b|DATE|AreaCode|UserId	1
		long viewTime = 0;
		long viewInterval = 0;
		String[] strkey = key.toString().trim().split("\\|",-1);
		if (strkey[0].equals("a")){
			for (Text value : values){
				viewTime += 1;
				viewInterval += Long.valueOf(value.toString());
			}
			BigInteger bivt = BigInteger.valueOf(viewTime);
			BigInteger bivi = BigInteger.valueOf(viewInterval);
			keyText.set(key);
			valueText.set(bivt+"|"+bivi);
			context.write(keyText, valueText);
//			System.out.println("kv= "+keyText+"|"+valueText);
		}else{
			for (Text value : values){
				keyText.set(key);
				valueText.set(value);
			}
			context.write(keyText, valueText);
//			System.out.println("kv= "+keyText+"|"+valueText);
		}
		
	}
}
