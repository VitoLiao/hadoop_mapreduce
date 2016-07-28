package com.channel.mr.ChnLinked;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelLinkedReducer91 extends Reducer<Text, Text, Text, NullWritable> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// input
		// DATE|CONTENTTYPE|sourceCode|CODE|LINKTYPE value
		// output
		keyText.set(key);
		String tmp = "";
		for(Text value : values){
//			valueText.set(value);
			tmp = value.toString();
		}
		valueText.set(tmp);
		
		context.write(valueText, NullWritable.get());
		
	}
}