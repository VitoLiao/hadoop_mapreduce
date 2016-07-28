package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelLinkedMapper91 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// input
		// DATE|CONTENTTYPE|TYPE|sourceCode|CODE| L1|L2|LINKTYPE
		// output
		// DATE|CONTENTTYPE|sourceCode|CODE|LINKTYPE value
		if(value.getLength()>0){
			String[] str = value.toString().split("\\|",-1);
			keyText.set(str[0]+"|"+str[1]+"|"+str[3]+"|"+str[4]+"|"+str[7]);
			valueText.set(value);
			context.write(keyText, valueText);
		}
	}
}

