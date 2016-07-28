package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelLinkedReducer2 extends Reducer<Text, Text, Text, Text>{
	
	Text valueText = new Text();
	private Text keyText = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//		CHANNELCODE MEDIACODE|STARTTIME|STOPTIME
		keyText.set(key);
		for(Text value : values){
			valueText.set(value);
			context.write(keyText, valueText);
		}
	}
}