package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelLinkedReducer8 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// input
		// DATE|CONTENTTYPE|TYPE|USERID| CODE|TIMEINTERVAL|LINKTYPE
		// output
		// DATE|CONTENTTYPE|TYPE|USERID###CODE|TIMEINTERVAL|LINKTYPE##...###0##1

		StringBuffer aBuffer = new StringBuffer();
		int sum = 0;
		for (Text value : values) {
			sum += 1;
			if (aBuffer.length() == 0) {
				aBuffer.append(value.toString());
			} else {
				aBuffer.append("##" + value.toString());
			}
		}
		if (sum > 1) {
			keyText.set(key + "###" + aBuffer.toString());
			valueText.set(String.valueOf(sum));
			context.write(keyText, valueText);
		}
		// if (sum > 1) {
		// int Len = 100;
		// if (sum <= 500) {
		// Len = 50;
		// } else if (sum > 500 && sum <= 1000) {
		// Len = 20;
		// } else if (sum > 1000 && sum <= 2000) {
		// Len = 20;
		// } else if (sum > 2000 && sum <= 5000) {
		// Len = 18;
		// } else if (sum > 5000 && sum <= 8000) {
		// Len = 15;
		// } else if (sum > 8000 && sum <= 10000) {
		// Len = 15;
		// } else if (sum > 10000) {
		// Len = 15;
		// }
		// keyText.set(key + "###" + aBuffer.toString());
		// int seperateA = 0;
		// int seperateB = 0;
		// while (seperateA < sum) {
		// seperateB += Len;
		// if (seperateB >= sum) {
		// // valueText.set(String.valueOf(seperateA)+"##"+String.valueOf(sum));
		// valueText.set(seperateA + "##" + sum);
		// context.write(keyText, valueText);
		// } else {
		// //
		// valueText.set(String.valueOf(seperateA)+"##"+String.valueOf(seperateB));
		// valueText.set(seperateA + "##" + seperateB);
		// context.write(keyText, valueText);
		// }
		// seperateA += Len;
		// }
		// }

		// valueText.set(aBuffer.toString());
		// context.write(keyText, valueText);

	}
}