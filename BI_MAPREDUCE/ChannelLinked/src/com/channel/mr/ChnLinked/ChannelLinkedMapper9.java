package com.channel.mr.ChnLinked;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ChannelLinkedMapper9 extends Mapper<LongWritable, Text, Text, Text> {

	private Text keyText = new Text();
	private Text valueText = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// input
		// DATE|CONTENTTYPE|TYPE|USERID###CODE|TIMEINTERVAL|LINKTYPE##...
		// DATE|CONTENTTYPE|TYPE|USERID###CODE|TIMEINTERVAL|LINKTYPE##...###0##1

		// output
		// DATE|CONTENTTYPE|TYPE|sourceCode|CODE| TIMEINTERVAL|LINKTYPE
		if (value.getLength() > 0) {
			// String[] strings = value.toString().split("###",-1);
			// String[] dStrings = strings[0].split("\\|",-1);
			// String[] aStrings = strings[1].split("##",-1);
			// String[] bStrings = strings[2].split("##",-1);
			//
			// for(int
			// i=Integer.valueOf(bStrings[0]);i<Integer.valueOf(bStrings[1]);i++){
			// String source = aStrings[i].split("\\|",-1)[0];
			// for (int j =0;j<aStrings.length;j++){
			// if(i==j){
			//
			// }else{
			// String[] str = aStrings[j].split("\\|",-1);
			// keyText.set(dStrings[0]+"|"+dStrings[1]+"|"+dStrings[2]+"|"+source+"|"+str[0]);
			// valueText.set(str[1]+"|"+str[2]);
			// context.write(keyText, valueText);
			// }
			// }
			// }

			String[] strings = value.toString().split("###", -1);
			 String[] dStrings = strings[0].split("\\|",-1);
			String[] aStrings = strings[1].split("##", -1);
			for (int i = 0; i < aStrings.length; i++) {
				String source = aStrings[i].split("\\|", -1)[0];
				for (int j = 0; j < aStrings.length; j++) {
					if (i == j) {

					} else {
						String[] str = aStrings[j].split("\\|", -1);
						keyText.set(dStrings[0]+"|"+dStrings[1]+"|"+dStrings[2]+"|"+ source + "|" + str[0]);
						valueText.set(str[1] + "|" + str[2]);
						context.write(keyText, valueText);
					}
				}
			}

		}
	}
}
