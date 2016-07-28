package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelLinkedReducer3 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// CODE DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL|STARTTIME|ENDTIME
		// CHANNELCODE MEDIACODE|STARTTIME|STOPTIME
		
//		keyText.set(key);
//		for(Text value : values){
//			valueText.set(value);
//			context.write(keyText, valueText);
//		}
		
		keyText.set(key);
		// int sum = 0;
		StringBuffer sBufferVL = new StringBuffer();
		StringBuffer sBufferSD = new StringBuffer();
		int asum = 0;
		int bsum = 0;
		for (Text value : values) {
			String[] str = value.toString().split("\\|", -1);
			if (str.length == 3) {
				asum += 1;
				if (sBufferSD.length() == 0) {
					sBufferSD.append(value);
				} else {
					sBufferSD.append("##" + value);
				}
			} else if (str.length == 8) {
				bsum += 1;
				if (sBufferVL.length() == 0) {
					sBufferVL.append(value);
				} else {
					sBufferVL.append("##" + value);
				}
			}
		}
		
//		if (asum > 0 && bsum > 0) {
//			valueText.set(asum+"##"+bsum);
//			context.write(keyText, valueText);
//		}
		
		if (asum > 0 && bsum > 0) {
			valueText.set(sBufferVL.toString() + "###" + sBufferSD.toString());
			context.write(keyText, valueText);
		}
	}
}