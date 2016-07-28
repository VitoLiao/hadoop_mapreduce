package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelLinkedMapper8 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// input
		// ot|DATE|CONTENTTYPE|TYPE|CODE|USERID|TIMEINTERVAL
		
		// output
		// DATE|CONTENTTYPE|TYPE|USERID| CODE|TIMEINTERVAL|LINKTYPE
		if(value.getLength()>0){
			String[] str = value.toString().split("\\|",-1);
			String linktype = "";
			if(str[0].equals("c")){
				linktype = "1";
			}else if(str[0].equals("t")){
				linktype = "3";
			}else if(str[0].equals("ot")){
				linktype = "5";
			}else if(str[0].equals("ct")){
				linktype = "7";
			}
			
			keyText.set(str[1]+"|"+str[2]+"|"+str[3]+"|"+str[5]);
			valueText.set(str[4]+"|"+str[6]+"|"+linktype);
			context.write(keyText, valueText);
		}
	}
}

