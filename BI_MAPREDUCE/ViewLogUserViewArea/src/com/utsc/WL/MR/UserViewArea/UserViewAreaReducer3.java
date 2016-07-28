package com.utsc.WL.MR.UserViewArea;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserViewAreaReducer3 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String[] bigboss = { "0", "0", "0", "0", "0", "0", "0", "0", "0", "0" };
		for (Text value : values) {
			String[] str = value.toString().split("\\|", -1);
			if (str[0].trim().equals("a")) {
				if (str[1].trim().equals("v")) {
					bigboss[7] = str[2];
				} else if (str[1].trim().equals("c")) {
					bigboss[8] = str[2];
				} else if (str[1].trim().equals("t")) {
					bigboss[9] = str[2];
				}
			} else if (str[0].trim().equals("b")) {
				if (str[1].trim().equals("v")) {
					bigboss[4] = str[2];
				} else if (str[1].trim().equals("c")) {
					bigboss[5] = str[2];
				} else if (str[1].trim().equals("t")) {
					bigboss[6] = str[2];
				}
			} else if (str[0].trim().equals("c")) {
				bigboss[2] = str[1];
			} else if (str[0].trim().equals("d")) {
				bigboss[3] = str[1];
			} else if (str[0].trim().equals("e")) {
				bigboss[0] = str[1];
			}
		}

		keyText.set(key);
		valueText.set(String.join("|", bigboss));
		context.write(keyText, valueText);
	}
}
