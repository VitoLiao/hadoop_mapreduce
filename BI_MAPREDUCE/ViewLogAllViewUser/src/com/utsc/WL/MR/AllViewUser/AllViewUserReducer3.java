package com.utsc.WL.MR.AllViewUser;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AllViewUserReducer3 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// intput
		// KEY: VALUE:
		// DATE|AreaCode|HdFlag HOUR|viewTime
		// output
		// DATE|AreaCode|HdFlag HOUR|viewTime
		String[] keystr = key.toString().trim().split("\\|", -1);
		String viewTimeAll = "0";
		String[] viewStr = { "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
				"0", "0", "0", "0", "0", "0" };
		for (Text value : values) {
			String[] str = value.toString().trim().split("\\|", -1);
			if (str[0].trim().equals("24")) {
				viewTimeAll = str[1];
			} else {
				viewStr[Integer.valueOf(str[0])] = str[1];
			}
		}
		keyText.set(keystr[0] + "|" + viewTimeAll + "|" + String.join("|", viewStr));
		valueText.set(keystr[1] + "|" + keystr[2]);
		context.write(keyText, valueText);
	}
}
