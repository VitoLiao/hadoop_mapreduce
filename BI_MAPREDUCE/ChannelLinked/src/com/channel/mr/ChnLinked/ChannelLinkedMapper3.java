package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelLinkedMapper3 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//input
		// ot|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL|STARTTIME|ENDTIME
		//	CHANNELCODE|MEDIACODE|STARTTIME|STOPTIME
		//output
		// CODE	DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL|STARTTIME|ENDTIME
		// CHANNELCODE MEDIACODE|STARTTIME|STOPTIME
		if (value.getLength() > 0) {
			String[] str = value.toString().split("\\|",-1);
			if(str.length == 9){
				keyText.set(str[4]);
				valueText.set(str[1]+"|"+str[2]+"|"+str[3]+"|"+str[4]+"|"+str[5]+"|"+str[6]+"|"+str[7]+"|"+str[8]);
				context.write(keyText, valueText);
			}else if (str.length == 4){
				keyText.set(str[0]);
				valueText.set(str[1]+"|"+str[2]+"|"+str[3]);
				context.write(keyText, valueText);
			}
		}
	}
}

