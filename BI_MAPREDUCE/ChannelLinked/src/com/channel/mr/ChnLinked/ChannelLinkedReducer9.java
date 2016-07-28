package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ChannelLinkedReducer9 extends Reducer<Text, Text, Text, Text> {

	Text valueText = new Text();
	private Text keyText = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// input
		// DATE|CONTENTTYPE|TYPE|sourceCode|CODE| TIMEINTERVAL|LINKTYPE
		// output
		// DATE|CONTENTTYPE|TYPE|sourceCode|CODE|L1|L2|LINKTYPE
		keyText.set(key);
		int unum = 0;
		int tnum = 0;
		String linktype = "";
		for (Text value : values){
			String[] str = value.toString().split("\\|",-1);
			unum += 1;
			tnum += Integer.valueOf(str[0]);
			linktype = str[1];
		}
		
		keyText.set( key);
		valueText.set(unum+"|"+tnum+"|"+linktype);
		context.write(keyText, valueText);
		
	}
}