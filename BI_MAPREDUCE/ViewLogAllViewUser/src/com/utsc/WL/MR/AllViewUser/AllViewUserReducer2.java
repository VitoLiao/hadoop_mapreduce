package com.utsc.WL.MR.AllViewUser;

import java.awt.geom.Rectangle2D;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AllViewUserReducer2 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|HOUR	UserId
		//or   DATE|AreaCode|HdFlag|24	UserId
		// output
		// DATE|AreaCode|HdFlag|HOUR	viewTime
		
		
		long viewTime = 0;
		keyText.set(key);
		for(Text value : values){
			viewTime += 1;
		}
		valueText.set(String.valueOf(viewTime));
		context.write(keyText, valueText);
		
	}
}
