package com.mr;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserViewAreaReducer2 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// input
		// key valut
		// DATE|AreaCode LogType|viewTime|viewInterval or b|DATE|AreaCode UserId
		// c:channel t:backwatch v:pointbrocast

		// for(Text value : values){
		// keyText.set(key);
		// valueText.set(value);
		// context.write(keyText, valueText);
		// }

		String[] str = key.toString().trim().split("\\|");
		long userAll = 0;
		long viewTimeAll = 0;
		long viewIntervalAll = 0;
		String[] strView = { "0", "0", "0", "0", "0", "0" };
		BigInteger bivta = BigInteger.valueOf(viewTimeAll);
		BigInteger bivia = BigInteger.valueOf(viewIntervalAll);
		for (Text value : values) {
			if (str[0].equals("b")) {
				userAll += 1;
			} else {
				String[] strings = value.toString().trim().split("\\|", -1);
				if (strings[0].equals("v")) {
					strView[0] = strings[1];
					strView[3] = strings[2];
				} else if (strings[0].equals("c")) {
					strView[1] = strings[1];
					strView[4] = strings[2];
				} else if (strings[0].equals("t")) {
					strView[2] = strings[1];
					strView[5] = strings[2];
				}
				viewTimeAll += Long.valueOf(strings[1]);
				viewIntervalAll += Long.valueOf(strings[2]);
				bivta = BigInteger.valueOf(viewTimeAll);
				bivia = BigInteger.valueOf(viewIntervalAll);
			}
//			System.out.println("kv= "+key+"|"+valueText.toString());
//			System.out.println("R2 kv = " + key + "|" + value);
		}
		if (str[0].equals("b")) {
			keyText.set(str[1] + "|" + str[2]);
			valueText.set(String.valueOf(userAll));
		}else{
			keyText.set(key);
			valueText.set(bivta + "|" + "0" + "|" + bivia + "|" + "0" + "|" + String.join("|", strView));
		}
		context.write(keyText, valueText);
	}
}
