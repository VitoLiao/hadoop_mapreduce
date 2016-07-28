package com.utsc.WL.MR.PartViewUser;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PartViewUserReducer2 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag|LogType|HOUR	UserId
		// output
		// DATE|AreaCode|HdFlag|LogType|HOUR	viewTime
		
		long viewTime = 0;
		for(Text value : values){
			viewTime += 1;
		}
		keyText.set(key);
		valueText.set(String.valueOf(viewTime));
		context.write(keyText, valueText);
	}
}
