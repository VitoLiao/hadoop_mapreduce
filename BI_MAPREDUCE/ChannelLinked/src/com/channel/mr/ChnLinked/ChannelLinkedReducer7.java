package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelLinkedReducer7 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// input
		// DATE|CONTENTTYPE|TYPE|CODE|PACKAGECODE/PROGRAMCODE| USERID|TIMEINTERVAL|LINKTYPE
		// output
		// DATE|CONTENTTYPE|TYPE|CODE|PACKAGECODE/PROGRAMCODE|L1|L2|LINKTYPE
		keyText.set(key);
		int unum = 0;
		int tnum = 0;
		String linktype = "";
		for(Text value : values){
			String[] str = value.toString().split("\\|",-1);
			unum += 1;
			tnum += 1;
			linktype = str[2];
		}
		
		keyText.set(key);
		valueText.set(unum+"|"+tnum+"|"+linktype);
		context.write(keyText, valueText);
	}
}