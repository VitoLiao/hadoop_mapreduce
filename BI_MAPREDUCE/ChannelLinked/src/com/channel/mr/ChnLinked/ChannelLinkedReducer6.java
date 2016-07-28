package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelLinkedReducer6 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// UID PACKAGECODE/PROGRAMCODE
		// UID ot|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		// keyText.set(key);
		boolean okFlagA = false;
		boolean okFlagB = false;
		StringBuffer aBufferSD = new StringBuffer();
		StringBuffer bBufferVL = new StringBuffer();
		for (Text value : values) {
			String[] str = value.toString().split("\\|", -1);
			if (str.length == 1) {
				okFlagA = true;
				if (aBufferSD.length() == 0) {
					aBufferSD.append(value.toString());
				} else {
					aBufferSD.append("##" + value.toString());
				}
			} else if (str.length == 7) {
				okFlagB = true;
				if (bBufferVL.length() == 0) {
					bBufferVL.append(value.toString());
				} else {
					bBufferVL.append("##" + value.toString());
				}
			}
		}
		
		if(okFlagA && okFlagB){
			keyText.set(aBufferSD.toString());
			valueText.set(bBufferVL.toString());
			context.write(keyText, valueText);
		}
	}
}