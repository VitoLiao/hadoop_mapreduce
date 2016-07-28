package com.utsc.WL.MR.UserViewArea;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserViewAreaReducer1 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// output
		// KEY: VALUE:
		// a|DATE|AreaCode|LogType TimeInterval or b|DATE|AreaCode|UserId 1
		// output
		// a|DATE|AreaCode|LogType|viewTime|viewInterval or
		// b|DATE|AreaCode|UserId 1

		long viewTimeIntervalSeperate = 0;
		long viewTimeSeperate = 0;
		long viewTimeIntervalSeperateAll = 0;
		long viewTimeSeperateAll = 0;
		String[] strkey = key.toString().trim().split("\\|", -1);

		// a : seperate view timeinterval
		// b : seperate user number
		// c : all view timeinterval
		// d : all disctint user number

		for (Text value : values) {
			if (strkey[0].trim().equals("a")) {
				viewTimeIntervalSeperate += Integer.valueOf(value.toString());
			} else if (strkey[0].trim().equals("b")) {
				viewTimeSeperate += 1;
			} else if (strkey[0].trim().equals("c")) {
				viewTimeIntervalSeperateAll += Integer.valueOf(value.toString());
			} else if (strkey[0].trim().equals("d")) {
				keyText.set(key);
				valueText.set(value);
			} else if (strkey[0].trim().equals("e")) {
				viewTimeSeperateAll += 1;
			}
		}

		if (strkey[0].trim().equals("a")) {
			keyText.set(key);
			valueText.set(String.valueOf(viewTimeIntervalSeperate));
			context.write(keyText, valueText);
		} else if (strkey[0].trim().equals("b")) {
			keyText.set(key);
			valueText.set(String.valueOf(viewTimeSeperate));
			context.write(keyText, valueText);
		} else if (strkey[0].trim().equals("c")) {
			keyText.set(key);
			valueText.set(String.valueOf(viewTimeIntervalSeperateAll));
			context.write(keyText, valueText);
		} else if (strkey[0].trim().equals("d")) {
			context.write(keyText, valueText);
		} else if (strkey[0].trim().equals("e")) {
			keyText.set(key);
			valueText.set(String.valueOf(viewTimeSeperateAll));
			context.write(keyText, valueText);
		}

	}
}
