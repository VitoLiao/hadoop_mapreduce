package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelLinkedMapper6 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// input
		// UID|PACKAGECODE/PROGRAMCODE
		// ot|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		// output
		// UID	PACKAGECODE/PROGRAMCODE
		// UID	ot|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		if(value.getLength()>0){
			String[] str = value.toString().split("\\|",-1);
			if(str.length == 2){
				keyText.set(str[0]);
				valueText.set(str[1]);
				context.write(keyText, valueText);
			}else if(str.length == 7){
				keyText.set(str[5]);
				valueText.set(value);
				context.write(keyText, valueText);
			}
		}
	}
}

